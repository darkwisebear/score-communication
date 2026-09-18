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

//! TEMPORARY stubs for `Runtime` associated types other than `Publisher`.
//!
//! Replace these by implementing `subscriber.rs`, `service_discovery.rs`, and
//! `producer_builder.rs` as described in the Plan B guide. Method bodies
//! `todo!()` so accidental use fails loudly.

#![allow(dead_code)]

use crate::registry::MockConsumerInfo;
use crate::runtime::MockRuntimeImpl;
use core::cmp::Ordering;
use core::fmt::Debug;
use core::future::Future;
use core::marker::PhantomData;
use core::ops::Deref;
use futures::stream::Stream;
use score_com_concept::{
    Builder, CommData, ConsumerBuilder, ConsumerDescriptor, InstanceSpecifier, Interface,
    ProducerBuilder, Result, Sample, SampleContainer, ServiceDiscovery, Subscriber, Subscription,
};

/// Placeholder sample so `Subscription`'s GAT compiles.
#[derive(Debug)]
pub struct StubSample<T> {
    data: T,
}

impl<T> Deref for StubSample<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T: CommData + Debug> PartialEq for StubSample<T> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<T: CommData + Debug> Eq for StubSample<T> {}

impl<T: CommData + Debug> PartialOrd for StubSample<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: CommData + Debug> Ord for StubSample<T> {
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl<T: CommData + Debug> Sample<T> for StubSample<T> {}

pub struct StubSubscriber<T> {
    _t: PhantomData<T>,
}

impl<T: CommData + Debug> Subscriber<T, MockRuntimeImpl> for StubSubscriber<T> {
    type Subscription = StubSubscription<T>;

    fn new(_identifier: &'static str, _instance_info: MockConsumerInfo) -> Result<Self> {
        todo!("TEMPORARY stub — replace in subscriber.rs slice")
    }

    fn subscribe(self, _max_num_samples: usize) -> Result<Self::Subscription> {
        todo!("TEMPORARY stub — replace in subscriber.rs slice")
    }
}

pub struct StubSubscription<T> {
    _t: PhantomData<T>,
}

impl<T: CommData + Debug> Subscription<T, MockRuntimeImpl> for StubSubscription<T> {
    type Subscriber = StubSubscriber<T>;
    type Sample<'a>
        = StubSample<T>
    where
        Self: 'a;

    fn unsubscribe(self) -> Self::Subscriber {
        todo!("TEMPORARY stub — replace in subscriber.rs slice")
    }

    fn try_receive<'a>(
        &'a self,
        _scratch: &'_ mut SampleContainer<Self::Sample<'a>>,
        _max_samples: usize,
    ) -> Result<usize> {
        todo!("TEMPORARY stub — replace in subscriber.rs slice")
    }

    fn cancellable_receive<'a>(
        &'a self,
        _scratch: SampleContainer<Self::Sample<'a>>,
        _new_samples: usize,
        _max_samples: usize,
        _cancellation: impl Future<Output = ()> + Send + 'static,
    ) -> impl Future<Output = (SampleContainer<Self::Sample<'a>>, Result<usize>)> + 'a {
        async { todo!("TEMPORARY stub — replace in subscriber.rs slice") }
    }

    fn to_stream<'a>(&'a mut self) -> impl Stream<Item = Result<Self::Sample<'a>>> + Unpin + 'a {
        futures::stream::empty()
    }
}

pub struct StubServiceDiscovery<I> {
    _i: PhantomData<I>,
}

impl<I> StubServiceDiscovery<I> {
    pub(crate) fn new() -> Self {
        Self { _i: PhantomData }
    }
}

impl<I: Interface + Send> ServiceDiscovery<I, MockRuntimeImpl> for StubServiceDiscovery<I> {
    type ConsumerBuilder = StubConsumerBuilder<I>;
    type ServiceEnumerator = Vec<StubConsumerBuilder<I>>;

    fn get_available_instances(&self) -> Result<Self::ServiceEnumerator> {
        todo!("TEMPORARY stub — replace in service_discovery.rs slice")
    }

    #[allow(clippy::manual_async_fn)]
    fn get_available_instances_async(
        &self,
    ) -> impl Future<Output = Result<Self::ServiceEnumerator>> + Send {
        async { todo!("TEMPORARY stub — replace in service_discovery.rs slice") }
    }
}

pub struct StubConsumerBuilder<I> {
    _i: PhantomData<I>,
}

impl<I: Interface> ConsumerDescriptor<MockRuntimeImpl> for StubConsumerBuilder<I> {
    fn get_instance_identifier(&self) -> &InstanceSpecifier {
        todo!("TEMPORARY stub — replace in service_discovery.rs slice")
    }
}

impl<I: Interface> ConsumerBuilder<I, MockRuntimeImpl> for StubConsumerBuilder<I> {}

impl<I: Interface> Builder<I::Consumer<MockRuntimeImpl>> for StubConsumerBuilder<I> {
    fn build(self) -> Result<I::Consumer<MockRuntimeImpl>> {
        todo!("TEMPORARY stub — replace in service_discovery.rs slice")
    }
}

pub struct StubProducerBuilder<I> {
    _i: PhantomData<I>,
}

impl<I> StubProducerBuilder<I> {
    pub(crate) fn new() -> Self {
        Self { _i: PhantomData }
    }
}

impl<I: Interface> ProducerBuilder<I, MockRuntimeImpl> for StubProducerBuilder<I> {}

impl<I: Interface> Builder<I::Producer<MockRuntimeImpl>> for StubProducerBuilder<I> {
    fn build(self) -> Result<I::Producer<MockRuntimeImpl>> {
        todo!("TEMPORARY stub — replace in producer_builder.rs slice")
    }
}
