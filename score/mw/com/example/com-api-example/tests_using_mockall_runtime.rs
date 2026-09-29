/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

//! Integration tests that mirror `tests_using_tokio_runtime.rs` but run against
//! the **mockall (Plan B)** runtime instead of Lola.
//!
//! This is the proof that `com-api-runtime-mockall` works: the *same* generic
//! helper functions (`create_producer`, `create_consumer_async`,
//! `async_data_sender_fn`, `stream_processor_fn`) that run unmodified against
//! `LolaRuntimeImpl` also run against `MockRuntimeImpl`, where **nothing
//! happens unless the test scripts it** through `expect_*()` on the policy
//! mocks. No config file and no backend are required, so the tests run in
//! normal CI.
//!
//! Run them with:
//! ```bash
//! bazel test //score/mw/com/example/com-api-example:com-api-example-mockall-runtime-test
//! ```

#[cfg(test)]
mod test {
    use com_api_gen::{Exhaust, Tire, VehicleConsumer, VehicleInterface, VehicleOfferedProducer};
    use futures::stream::StreamExt;
    use mockall::predicate::eq;
    use score_com::{
        AllocationFailureReason, Builder, FindServiceSpecifier, InstanceSpecifier,
        MockConsumerBuilderPolicy, MockProviderInfoPolicy, MockProducerBuilderPolicy,
        MockPublisherPolicy, MockRuntimeImpl, MockServiceDiscoveryPolicy,
        MockSubscriptionPolicy, MockSubscriberPolicy, OfferedProducer, PollOutcome, Producer,
        Publisher, Runtime, SampleMaybeUninit, SampleMut, ServiceDiscovery,
        SharedConsumerBuilder, Subscriber, Subscription,
    };
    use std::sync::{Arc, Mutex};

    const INITIAL_TIRE_PRESSURE: f32 = 1.0;
    const SAMPLE_COUNT: usize = 5;
    const SAMPLE_INTERVAL_MS: u64 = 1; // no real IPC to wait for; keep CI fast
    const PRODUCER_ITERATIONS: usize = 5;
    const CONSUMER_ITERATIONS: usize = 5;
    const SPEC_PATH: &str = "/Vehicle/Service1/Instance";

    fn spec() -> InstanceSpecifier {
        InstanceSpecifier::new(SPEC_PATH).expect("valid instance specifier")
    }

    fn expected_pressures() -> Vec<f32> {
        (0..PRODUCER_ITERATIONS)
            .map(|i| INITIAL_TIRE_PRESSURE + i as f32)
            .collect()
    }

    /// Collects every tire the `left_tire` publisher is scripted to accept,
    /// so the test can assert on the exact sends (`Tire` has no `PartialEq`).
    type SharedF32s = Arc<Mutex<Vec<f32>>>;

    // ---------------------------------------------------------------------
    // Helpers generic over `R: Runtime` — the SAME functions the Lola test
    // uses, proving unmodified user code runs against the mock runtime.
    // ---------------------------------------------------------------------

    fn create_producer<R: Runtime>(
        runtime: &R,
        service_id: InstanceSpecifier,
    ) -> VehicleOfferedProducer<R> {
        let producer_builder = runtime.producer_builder::<VehicleInterface>(service_id);
        let producer = producer_builder
            .build()
            .expect("Failed to build producer instance");
        producer.offer().expect("Failed to offer producer instance")
    }

    async fn create_consumer_async<R: Runtime>(
        runtime: &R,
        service_id: InstanceSpecifier,
    ) -> VehicleConsumer<R> {
        let consumer_discovery =
            runtime.find_service::<VehicleInterface>(FindServiceSpecifier::Specific(service_id));
        let available_service_instances = consumer_discovery
            .get_available_instances_async()
            .await
            .expect("Failed to get available service instances asynchronously");

        let handle_index = 0;
        let consumer_builder = available_service_instances
            .into_iter()
            .nth(handle_index)
            .expect("Failed to get consumer builder at specified handle index");

        consumer_builder
            .build()
            .expect("Failed to build consumer instance")
    }

    // sender: allocates + sends one tire per iteration
    async fn async_data_sender_fn<R: Runtime>(
        offered_producer: VehicleOfferedProducer<R>,
    ) -> VehicleOfferedProducer<R> {
        for i in 0..PRODUCER_ITERATIONS {
            let uninit_sample = match offered_producer.left_tire.allocate() {
                Ok(sample) => sample,
                Err(e) => {
                    eprintln!("[SENDER] Failed to allocate sample: {:?}", e);
                    continue;
                }
            };
            let sample = uninit_sample.write(Tire {
                pressure: INITIAL_TIRE_PRESSURE + i as f32,
            });
            match sample.send() {
                Ok(_) => (),
                Err(e) => eprintln!("[SENDER] Failed to send sample: {:?}", e),
            }
            println!(
                "[SENDER] Sent sample with pressure: {}",
                INITIAL_TIRE_PRESSURE + i as f32
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(SAMPLE_INTERVAL_MS)).await;
        }
        offered_producer
    }

    // receiver: drains the subscription as a stream for CONSUMER_ITERATIONS items
    async fn stream_processor_fn<R: Runtime>(
        mut subscribed: impl Subscription<Tire, R>,
        received: SharedF32s,
    ) {
        let mut stream = subscribed.to_stream();
        let mut cnt = CONSUMER_ITERATIONS;
        println!("[RECEIVER] Stream processor started");
        while cnt > 0 {
            match tokio::time::timeout(
                tokio::time::Duration::from_millis(500),
                stream.next(),
            )
            .await
            {
                Ok(Some(Ok(sample))) => {
                    received.lock().expect("poisoned").push(sample.pressure);
                    println!(
                        "[RECEIVER] Stream received sample: {:.2} psi",
                        sample.pressure
                    )
                }
                Ok(Some(Err(e))) => eprintln!("[RECEIVER] Stream error: {:?}", e),
                Ok(None) => break,
                Err(_) => {
                    eprintln!("[RECEIVER] Timeout while waiting for stream sample");
                    break;
                }
            }
            cnt -= 1;
        }
    }

    /// Build the `left_tire` publisher policy: scripted to allocate
    /// `PRODUCER_ITERATIONS` times and to record every sent tire into `sent`.
    fn left_tire_policy(sent: SharedF32s) -> MockPublisherPolicy<Tire> {
        let mut policy = MockPublisherPolicy::<Tire>::new();
        policy
            .expect_allocate()
            .times(PRODUCER_ITERATIONS)
            .returning(|| Ok(()));
        policy
            .expect_send()
            .returning(move |tire: Tire| {
                sent.lock().expect("poisoned").push(tire.pressure);
                Ok(())
            });
        policy
    }

    /// Register every policy a full producer/consumer round trip will pop.
    fn script_round_trip(runtime: &MockRuntimeImpl, sent: SharedF32s) {
        let spec = spec();
        let registry = runtime.registry().clone();

        // ---- producer side ----------------------------------------------
        let mut producer_builder = MockProducerBuilderPolicy::new();
        producer_builder.expect_build().times(1).returning(|| Ok(()));
        runtime.expect_producer_builder(&spec, producer_builder);

        let mut provider = MockProviderInfoPolicy::new();
        provider.expect_offer_service().times(1).returning(|| Ok(()));
        provider
            .expect_stop_offer_service()
            .times(1)
            .returning(|| Ok(()));
        runtime.expect_provider_info(&spec, provider);

        // `offer()` creates a publisher for *every* event; script `left_tire`
        // and leave `exhaust` unscripted (any call panics — desired strictness).
        registry.expect_publisher(&spec, "left_tire", left_tire_policy(sent));
        registry.expect_publisher(
            &spec,
            "exhaust",
            MockPublisherPolicy::<Exhaust>::new(),
        );

        // ---- consumer side ----------------------------------------------
        // `VehicleConsumer::new` builds a subscriber for *every* event.
        let mut left_tire_sub = MockSubscriberPolicy::<Tire>::new();
        left_tire_sub
            .expect_subscribe()
            .with(eq(SAMPLE_COUNT))
            .times(1)
            .returning(move |max_num_samples| {
                assert_eq!(max_num_samples, SAMPLE_COUNT);
                Ok(())
            });
        registry.expect_subscriber(&spec, "left_tire", left_tire_sub);
        registry.expect_subscriber(&spec, "exhaust", MockSubscriberPolicy::<Exhaust>::new());

        // The stream replays the scripted tires one at a time, then parks.
        let mut subscription = MockSubscriptionPolicy::<Tire>::new();
        let mut batch = expected_pressures().into_iter();
        subscription
            .expect_poll_samples()
            .returning(move |_max| Ok(match batch.next() {
                Some(p) => PollOutcome::Ready(vec![Tire { pressure: p }]),
                None => PollOutcome::Idle,
            }));
        subscription.expect_unsubscribe().return_const(());
        registry.expect_subscription(&spec, "left_tire", subscription);

        // ---- service discovery ------------------------------------------
        let mut consumer_builder = MockConsumerBuilderPolicy::new();
        consumer_builder.expect_build().times(1).returning(|| Ok(()));
        let builder =
            SharedConsumerBuilder::from_policy(spec.clone(), registry.clone(), consumer_builder);
        let mut discovery = MockServiceDiscoveryPolicy::new();
        discovery
            .expect_builders()
            .returning(move || Ok(vec![builder.clone()]));
        runtime.expect_service_discovery(&spec, discovery);
    }

    // ---------------------------------------------------------------------
    // Test 1: mirror `stream_and_send_using_multi_thread` — the whole
    // producer/consumer round trip runs against the mock runtime with every
    // interaction scripted.
    // ---------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_and_send_using_mockall() {
        println!("Starting async subscription test with mockall runtime");
        let runtime = Arc::new(MockRuntimeImpl::new());
        let sent: SharedF32s = Arc::new(Mutex::new(Vec::new()));
        script_round_trip(&runtime, sent.clone());
        let spec = spec();

        // starting service discovery in async way, so that it can be
        // discovered while the consumer is waiting for the producer to offer.
        let consumer_runtime = runtime.clone();
        let consumer_spec = spec.clone();
        let consumer = tokio::spawn(async move {
            create_consumer_async(&*consumer_runtime, consumer_spec).await
        });

        let producer = create_producer(&*runtime, spec.clone());
        let sender_join_handle = tokio::spawn(async_data_sender_fn(producer));

        let consumer = consumer.await.expect("Failed to create consumer");
        // Subscribe to one event
        let subscribed = consumer.left_tire.subscribe(SAMPLE_COUNT).unwrap();

        let received: SharedF32s = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let processor_join_handle =
            tokio::spawn(async move { stream_processor_fn(subscribed, received_clone).await });
        processor_join_handle
            .await
            .expect("Error returned from stream processor task");

        let producer = sender_join_handle.await.expect("Error returned from sender task");

        match producer.unoffer() {
            Ok(_) => println!("Successfully unoffered the service"),
            Err(e) => eprintln!("Failed to unoffer: {:?}", e),
        }

        // Every scripted send was captured with the exact pressures.
        assert_eq!(sent.lock().expect("poisoned").clone(), expected_pressures());
        // The consumer replayed every scripted tire through the stream.
        assert_eq!(
            received.lock().expect("poisoned").clone(),
            expected_pressures()
        );

        println!("=== Async subscription test with mockall runtime completed ===\n");
    }

    // ---------------------------------------------------------------------
    // Test 2: error injection — the mock's raison d'être. `allocate` is
    // scripted to fail, so the (unmodified) sender logs and sends nothing;
    // the stream still replays every scripted tire, demonstrating that
    // scripted behavior fully controls the mock.
    // ---------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn error_injection_allocate_fails_and_sender_continues() {
        println!("Starting error-injection test with mockall runtime");
        let runtime = Arc::new(MockRuntimeImpl::new());
        let spec = spec();
        let registry = runtime.registry().clone();
        let sent: SharedF32s = Arc::new(Mutex::new(Vec::new()));

        // ---- producer side ----------------------------------------------
        let mut producer_builder = MockProducerBuilderPolicy::new();
        producer_builder.expect_build().times(1).returning(|| Ok(()));
        runtime.expect_producer_builder(&spec, producer_builder);

        let mut provider = MockProviderInfoPolicy::new();
        provider.expect_offer_service().times(1).returning(|| Ok(()));
        provider
            .expect_stop_offer_service()
            .times(1)
            .returning(|| Ok(()));
        runtime.expect_provider_info(&spec, provider);

        // `allocate` is scripted to fail on every call.
        let mut left_tire = MockPublisherPolicy::<Tire>::new();
        left_tire
            .expect_allocate()
            .times(PRODUCER_ITERATIONS)
            .returning(|| {
                Err(score_com::Error::AllocateError(
                    AllocationFailureReason::InvalidRequest,
                ))
            });
        registry.expect_publisher(&spec, "left_tire", left_tire);
        registry.expect_publisher(
            &spec,
            "exhaust",
            MockPublisherPolicy::<Exhaust>::new(),
        );

        // ---- consumer side ----------------------------------------------
        let mut left_tire_sub = MockSubscriberPolicy::<Tire>::new();
        left_tire_sub
            .expect_subscribe()
            .with(eq(SAMPLE_COUNT))
            .times(1)
            .returning(move |max_num_samples| {
                assert_eq!(max_num_samples, SAMPLE_COUNT);
                Ok(())
            });
        registry.expect_subscriber(&spec, "left_tire", left_tire_sub);
        registry.expect_subscriber(&spec, "exhaust", MockSubscriberPolicy::<Exhaust>::new());

        let mut subscription = MockSubscriptionPolicy::<Tire>::new();
        let mut batch = expected_pressures().into_iter();
        subscription
            .expect_poll_samples()
            .returning(move |_max| Ok(match batch.next() {
                Some(p) => PollOutcome::Ready(vec![Tire { pressure: p }]),
                None => PollOutcome::Idle,
            }));
        subscription.expect_unsubscribe().return_const(());
        registry.expect_subscription(&spec, "left_tire", subscription);

        // ---- service discovery ------------------------------------------
        let mut consumer_builder = MockConsumerBuilderPolicy::new();
        consumer_builder.expect_build().times(1).returning(|| Ok(()));
        let builder =
            SharedConsumerBuilder::from_policy(spec.clone(), registry.clone(), consumer_builder);
        let mut discovery = MockServiceDiscoveryPolicy::new();
        discovery
            .expect_builders()
            .returning(move || Ok(vec![builder.clone()]));
        runtime.expect_service_discovery(&spec, discovery);

        // ---- exercise ---------------------------------------------------
        let consumer_runtime = runtime.clone();
        let consumer_spec = spec.clone();
        let consumer = tokio::spawn(async move {
            create_consumer_async(&*consumer_runtime, consumer_spec).await
        });
        let producer = create_producer(&*runtime, spec.clone());
        let sender_join_handle = tokio::spawn(async_data_sender_fn(producer));
        let consumer = consumer.await.expect("Failed to create consumer");
        let subscribed = consumer.left_tire.subscribe(SAMPLE_COUNT).unwrap();
        let received: SharedF32s = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let processor_join_handle =
            tokio::spawn(async move { stream_processor_fn(subscribed, received_clone).await });
        processor_join_handle
            .await
            .expect("Error returned from stream processor task");
        let producer = sender_join_handle.await.expect("Error returned from sender task");
        let _ = producer.unoffer();

        // Allocation failed, so nothing was sent...
        assert!(sent.lock().expect("poisoned").is_empty());
        // ...but the scripted stream still replays every tire.
        assert_eq!(
            received.lock().expect("poisoned").clone(),
            expected_pressures()
        );

        println!("=== Error-injection test with mockall runtime completed ===\n");
    }
}
