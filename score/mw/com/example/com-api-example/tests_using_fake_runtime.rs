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
//! the **functional fake (Plan A)** runtime instead of Lola or mockall.
//!
//! Unlike the mockall test, *nothing is scripted*: the same generic helper
//! functions (`create_producer`, `create_consumer_async`, `async_data_sender_fn`,
//! `stream_processor_fn`) that run unmodified against `LolaRuntimeImpl` and
//! `MockRuntimeImpl` run unmodified against [`FakeRuntimeImpl`], and the tire
//! pressures sent by the producer are **actually delivered** to the consumer's
//! subscription over the in-memory registry. No config file and no backend are
//! required, so the tests run in normal CI.
//!
//! Run them with:
//! ```bash
//! bazel test //score/mw/com/example/com-api-example:com-api-example-fake-runtime-test
//! ```

#[cfg(test)]
mod test {
    use com_api_gen::{Tire, VehicleConsumer, VehicleInterface, VehicleOfferedProducer};
    use futures::stream::StreamExt;
    use score_com::{
        Builder, FakeRuntimeImpl, FindServiceSpecifier, InstanceSpecifier, OfferedProducer,
        Producer, Publisher, Runtime, SampleMaybeUninit, SampleMut, ServiceDiscovery,
        Subscription, Subscriber,
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

    type SharedF32s = Arc<Mutex<Vec<f32>>>;

    // ---------------------------------------------------------------------
    // Helpers generic over `R: Runtime` — the SAME functions the Lola and
    // mockall tests use, proving unmodified user code runs against the fake
    // runtime.
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

    // ---------------------------------------------------------------------
    // The whole producer/consumer round trip runs against the functional fake
    // runtime. No scripting: the pressures the sender publishes are the exact
    // pressures the consumer's stream receives, delivered by the in-memory
    // registry.
    // ---------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_and_send_using_fake() {
        println!("Starting async subscription test with fake runtime");
        let runtime = Arc::new(FakeRuntimeImpl::new());
        let spec = spec();

        // Start service discovery asynchronously so the consumer can be created
        // even though the producer has not yet offered — discovery parks until
        // the offer arrives, exactly like the Lola test.
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

        match producer.unoffer() {
            Ok(_) => println!("Successfully unoffered the service"),
            Err(e) => eprintln!("Failed to unoffer: {:?}", e),
        }

        // The pressures that crossed the wire equal the pressures that were
        // published — the fake backend really delivered them.
        assert_eq!(
            received.lock().expect("poisoned").clone(),
            expected_pressures()
        );

        println!("=== Async subscription test with fake runtime completed ===\n");
    }
}
