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

//! Functional fake backend for `score_com_concept::Runtime` (Plan A).
//!
//! Unlike the classical mockall backend (Plan B), where *nothing happens
//! unless a test scripts it*, this backend is a **working in-memory pub/sub
//! simulation**: samples published by an offered producer are actually
//! retained per event and delivered to matching subscriptions, with real
//! service discovery (a consumer discovers an instance once it is offered,
//! even if the consumer is created first). It is a drop-in backend for
//! exercising application business logic — including `interface!`-generated
//! producer/consumer code, which runs unmodified — without any real IPC or
//! manual expectation scripting.
//!
//! # How the endpoints exchange data
//!
//! Everything a [`FakeRuntimeImpl`] hands out shares one [`FakeRegistry`]
//! (behind `Arc`). Each `(interface, instance, event)` endpoint owns one
//! retained broadcast queue; a [`FakePublisher`](crate::publisher) appends to
//! it and a [`FakeSubscription`](crate::subscriber) drains it from its own
//! cursor. Cloning a runtime (or any handle) shares the same registry, so
//! producers and consumers created through different clones interoperate.
//!
//! # Using it
//!
//! ```ignore
//! let runtime = FakeRuntimeImpl::new();
//! let producer_builder = runtime.producer_builder::<MyInterface>(spec);
//! let offered = producer_builder.build()?.offer()?;
//! offered.my_event.send(value)?; // actually delivered to subscribers
//!
//! let discovery = runtime.find_service::<MyInterface>(FindServiceSpecifier::Any);
//! let consumer = discovery.get_available_instances_async().await?
//!     .into_iter().next().unwrap().build()?;
//! let received: Vec<MyEvent> = consumer
//!     .my_event.subscribe(5)?.to_stream().collect().await;
//! ```
//!
//! The mirrored integration test
//! `score/mw/com/example/com-api-example/tests_using_fake_runtime.rs` runs the
//! same `Tire`/`Vehicle` round trip as the tokio and mockall tests, but with
//! no expectations scripted at all.

mod consumer;
mod producer_builder;
mod provider_info;
mod publisher;
mod registry;
mod runtime;
mod subscriber;

pub use consumer::{FakeConsumerBuilder, FakeConsumerDiscovery, FakeConsumerInfo};
pub use producer_builder::FakeProducerBuilder;
pub use provider_info::FakeProviderInfo;
pub use publisher::{FakePublisher, FakeSampleMaybeUninit, FakeSampleMut};
pub use registry::{EventQueue, FakeRegistry};
pub use runtime::{FakeRuntimeBuilderImpl, FakeRuntimeImpl};
pub use subscriber::{FakeSample, FakeSubscription, FakeSubscriber};
