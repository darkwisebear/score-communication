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

//! Classical mockall backend for `score_com_concept::Runtime` (Plan B).
//!
//! Nothing happens unless a test scripts it through `expect_*()` on the policy
//! mocks. Two mockall limitations shape the design:
//!
//! 1. **GAT wall.** mockall 0.14 cannot `#[automock]` a trait with generic
//!    associated types (`Runtime`, `Publisher::SampleMaybeUninit<'a>`,
//!    `Subscription::Sample<'a>`). Those traits are hand-written wrappers whose
//!    only scriptable decisions live on lifetime-free `*Policy` traits that
//!    *are* `#[automock]`-ed. See [`publisher`] and [`subscriber`] for the two
//!    GAT walls.
//! 2. **Macro-constructed endpoints.** `interface!` constructs
//!    `Publisher`/`Subscriber` internally from `R::ProviderInfo` /
//!    `R::ConsumerInfo`, so tests cannot hand in pre-configured mocks. Instead
//!    both info types carry the shared [`MockRegistry`], and tests register
//!    policies up front keyed by `(instance, event)` — or per instance for
//!    service discovery, producer builder and provider info; the generated
//!    constructors pop them out.
//!
//! # Scripting a full producer/consumer round trip
//!
//! ```ignore
//! use mockall::predicate::eq;
//!
//! let runtime = MockRuntimeImpl::new();
//! let spec = InstanceSpecifier::new("/Vehicle/Service1/Instance")?;
//!
//! // Producer side: builder + offer + publishers (created by `offer()`).
//! let mut producer_builder = MockProducerBuilderPolicy::new();
//! producer_builder.expect_build().times(1).returning(|| Ok(()));
//! runtime.expect_producer_builder(&spec, producer_builder);
//!
//! let mut offer = MockProviderInfoPolicy::new();
//! offer.expect_offer_service().times(1).returning(|| Ok(()));
//! offer.expect_stop_offer_service().times(1).returning(|| Ok(()));
//! runtime.expect_provider_info(&spec, offer);
//!
//! let mut left_tire = MockPublisherPolicy::<Tire>::new();
//! left_tire.expect_allocate().returning(|| Ok(()));
//! left_tire
//!     .expect_send()
//!     .with(eq(Tire { pressure: 1.0 }))
//!     .returning(|_| Ok(()));
//! runtime.registry().expect_publisher(&spec, "left_tire", left_tire);
//! // `offer()` also creates a publisher for every other event (e.g. `exhaust`);
//! // register policies for them as well (no expectations ⇒ any call panics).
//!
//! // Consumer side: discovery + builder + subscriber + subscription.
//! let mut consumer_builder = MockConsumerBuilderPolicy::new();
//! consumer_builder.expect_build().times(1).returning(|| Ok(()));
//! let mut discovery = MockServiceDiscoveryPolicy::new();
//! discovery.expect_builders().returning(|| Ok(vec![SharedConsumerBuilder::from_policy(
//!     spec.clone(),
//!     runtime.registry().clone(),
//!     consumer_builder,
//! )]));
//! runtime.expect_service_discovery(&spec, discovery);
//!
//! let mut subscriber = MockSubscriberPolicy::<Tire>::new();
//! subscriber.expect_subscribe().with(eq(5)).times(1).returning(|| Ok(()));
//! runtime.registry().expect_subscriber(&spec, "left_tire", subscriber);
//! let mut subscription = MockSubscriptionPolicy::<Tire>::new();
//! subscription.expect_poll_samples().returning(|_| Ok(PollOutcome::Idle));
//! runtime.registry().expect_subscription(&spec, "left_tire", subscription);
//! # Ok::<(), score_com_concept::Error>(())
//! ```
//!
//! The mirrored integration test
//! `score/mw/com/example/com-api-example/tests_using_mockall_runtime.rs` runs
//! this end to end against the `Tire`/`Vehicle` example interface.

// Lifetime parameters on sample types are required by the Publisher/Subscription
// GATs even when the inner fields own their data.
#![allow(clippy::needless_lifetimes)]

mod producer_builder;
mod provider_info;
mod publisher;
mod registry;
mod runtime;
mod service_discovery;
mod subscriber;

pub use producer_builder::{
    MockProducerBuilderImpl, MockProducerBuilderPolicy, ProducerBuilderPolicy,
};
pub use provider_info::{MockProviderInfoPolicy, ProviderInfoHandle, ProviderInfoPolicy};
pub use publisher::{
    MockPublisherImpl, MockPublisherPolicy, MockSampleMaybeUninit, MockSampleMut, PublisherPolicy,
};
pub use registry::{MockConsumerInfo, MockRegistry};
pub use runtime::{MockRuntimeBuilderImpl, MockRuntimeImpl};
pub use service_discovery::{
    MockConsumerBuilderImpl, MockConsumerBuilderPolicy, MockServiceDiscoveryImpl,
    MockServiceDiscoveryPolicy, ServiceDiscoveryPolicy, SharedConsumerBuilder,
};
pub use subscriber::{
    MockSample, MockSubscriptionImpl, MockSubscriptionPolicy, MockSubscriberImpl,
    MockSubscriberPolicy, PollOutcome, SubscriberPolicy, SubscriptionPolicy,
};

// Error reason enums (re-exported so tests can script injected failures, e.g.
// `Error::AllocateError(AllocationFailureReason::InvalidRequest)`). `Error`
// itself stays owned by `score_com` to avoid a glob collision in the shim.
pub use score_com_concept::{
    AllocationFailureReason, ConsumerFailedReason, EventFailedReason, ProducerFailedReason,
    ReceiveFailedReason, ServiceFailedReason,
};
