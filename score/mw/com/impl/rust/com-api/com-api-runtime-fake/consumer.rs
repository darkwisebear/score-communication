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

//! Consumer-side types for the fake backend: consumer info, consumer
//! builder, and service discovery.
//!
//! [`FakeConsumerInfo`] carries the `(interface, instance, registry)`
//! coordinates every consumer-side handle needs; all clones share the same
//! registry, so the [`FakeSubscriber`](crate::subscriber::FakeSubscriber)
//! handles that an `interface!`-generated consumer creates always operate on
//! the same queues as the corresponding publisher.
//!
//! Service discovery is **real**: `get_available_instances` reports the
//! currently offered instances of the interface, and
//! `get_available_instances_async` polls until at least one matching instance
//! is offered — so a consumer created before the producer offers its service
//! still discovers it (the timing the Lola-based example tests rely on).

use crate::registry::FakeRegistry;
use crate::runtime::FakeRuntimeImpl;
use core::future::Future;
use core::marker::PhantomData;
use core::task::{Context, Poll};
use score_com_concept::{
    Builder, ConsumerBuilder, ConsumerDescriptor, InstanceSpecifier, Interface, Result,
    ServiceDiscovery,
};
use std::fmt::Debug;

/// `Runtime::ConsumerInfo` for the fake backend.
///
/// `interface!` clones this once per event and passes each clone to
/// `Subscriber::new`, so it is plain `Clone + Send` data (no mockable
/// decisions on the consumer side need to be scripted; the subscription
/// behavior itself is real and queue-backed).
#[derive(Clone, Debug)]
pub struct FakeConsumerInfo {
    pub(crate) interface_id: String,
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: FakeRegistry,
}

impl FakeConsumerInfo {
    /// Construct from coordinates; internal to the fake runtime.
    pub(crate) fn new(
        interface_id: &str,
        instance: InstanceSpecifier,
        registry: FakeRegistry,
    ) -> Self {
        Self {
            interface_id: interface_id.to_owned(),
            instance,
            registry,
        }
    }

    /// The interface this consumer instance belongs to.
    pub fn interface_id(&self) -> &str {
        &self.interface_id
    }

    /// The instance specifier of this consumer.
    pub fn instance(&self) -> &InstanceSpecifier {
        &self.instance
    }

    /// The registry this consumer operates on.
    pub fn registry(&self) -> &FakeRegistry {
        &self.registry
    }
}

/// Builder of a discovered consumer instance; the value
/// `get_available_instances` hands out. `build()` constructs the
/// `interface!`-generated consumer type `I::Consumer` through the
/// [`score_com_concept::Consumer`] trait (the macro generates the concrete
/// struct, so there is nothing fake to construct here).
pub struct FakeConsumerBuilder<I: Interface + Send> {
    pub(crate) interface_id: String,
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: FakeRegistry,
    pub(crate) _i: PhantomData<I>,
}

impl<I: Interface + Send> Clone for FakeConsumerBuilder<I> {
    fn clone(&self) -> Self {
        Self {
            interface_id: self.interface_id.clone(),
            instance: self.instance.clone(),
            registry: self.registry.clone(),
            _i: PhantomData,
        }
    }
}

impl<I: Interface + Send> Debug for FakeConsumerBuilder<I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FakeConsumerBuilder")
            .field("interface_id", &self.interface_id)
            .field("instance", &self.instance)
            .finish()
    }
}

impl<I: Interface + Send> ConsumerDescriptor<FakeRuntimeImpl> for FakeConsumerBuilder<I> {
    fn get_instance_identifier(&self) -> &InstanceSpecifier {
        &self.instance
    }
}

impl<I: Interface + Send> Builder<I::Consumer<FakeRuntimeImpl>> for FakeConsumerBuilder<I> {
    fn build(self) -> Result<I::Consumer<FakeRuntimeImpl>> {
        let info = FakeConsumerInfo::new(
            &self.interface_id,
            self.instance.clone(),
            self.registry.clone(),
        );
        // `Consumer::new` returns `Self` directly (not a `Result`); it builds
        // the `interface!`-generated consumer, constructing one `Subscriber`
        // per event from the info.
        Ok(<I::Consumer<FakeRuntimeImpl> as score_com_concept::Consumer<
            FakeRuntimeImpl,
        >>::new(info))
    }
}

impl<I: Interface + Send> ConsumerBuilder<I, FakeRuntimeImpl> for FakeConsumerBuilder<I> {}

/// Service discovery of the fake backend, for one interface.
///
/// `FindServiceSpecifier` is neither `Clone` nor `Send`, so the discovery
/// handle eagerly converts it into an owned `Option<String>`: `Some(spec)`
/// for `Specific`, `None` for `Any`.
pub struct FakeConsumerDiscovery<I: Interface + Send> {
    pub(crate) interface_id: &'static str,
    pub(crate) registry: FakeRegistry,
    /// `Some(spec)` to match a specific instance, `None` for any.
    pub(crate) wanted_instance: Option<String>,
    pub(crate) _i: PhantomData<I>,
}

impl<I: Interface + Send> FakeConsumerDiscovery<I> {
    /// Match the currently offered instances of this interface against the
    /// wanted instance (`None` = any).
    fn matching(&self) -> Vec<InstanceSpecifier> {
        let offered = self.registry.offered_instances(self.interface_id);
        match &self.wanted_instance {
            None => offered,
            Some(wanted) => offered
                .into_iter()
                .filter(|candidate| candidate.as_ref() == wanted.as_str())
                .collect(),
        }
    }
}

impl<I: Interface + Send> Debug for FakeConsumerDiscovery<I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FakeConsumerDiscovery")
            .field("interface_id", &self.interface_id)
            .field("wanted_instance", &self.wanted_instance)
            .finish()
    }
}

impl<I: Interface + Send> ServiceDiscovery<I, FakeRuntimeImpl> for FakeConsumerDiscovery<I> {
    type ConsumerBuilder = FakeConsumerBuilder<I>;
    type ServiceEnumerator = Vec<FakeConsumerBuilder<I>>;

    fn get_available_instances(&self) -> Result<Self::ServiceEnumerator> {
        Ok(self
            .matching()
            .into_iter()
            .map(|instance| FakeConsumerBuilder {
                interface_id: self.interface_id.to_owned(),
                instance,
                registry: self.registry.clone(),
                _i: PhantomData,
            })
            .collect())
    }

    #[allow(clippy::manual_async_fn)]
    fn get_available_instances_async(
        &self,
    ) -> impl Future<Output = Result<Self::ServiceEnumerator>> + Send {
        // The future outlives `self`, so everything it captures must be
        // owned.
        let interface_id: String = self.interface_id.to_owned();
        let registry = self.registry.clone();
        let wanted_instance = self.wanted_instance.clone();
        futures::future::poll_fn(move |cx: &mut Context<'_>| {
            let offered = registry.offered_instances(&interface_id);
            let matching: Vec<InstanceSpecifier> = match &wanted_instance {
                None => offered,
                Some(wanted) => offered
                    .into_iter()
                    .filter(|candidate| candidate.as_ref() == wanted.as_str())
                    .collect(),
            };
            if !matching.is_empty() {
                return Poll::Ready(Ok(
                    matching
                        .into_iter()
                        .map(|instance| FakeConsumerBuilder {
                            interface_id: interface_id.clone(),
                            instance,
                            registry: registry.clone(),
                            _i: PhantomData,
                        })
                        .collect(),
                ));
            }
            // Park until an instance of this interface is (re-)offered; the
            // registry wakes every parked discovery on the interface.
            registry.add_interface_waiter(&interface_id, cx.waker().clone());
            Poll::Pending
        })
    }
}
