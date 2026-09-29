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

//! Shared, type-erased store of pre-configured policy mocks.
//!
//! `interface!` constructs `Publisher`/`Subscriber` endpoints internally from
//! `R::ProviderInfo` / `R::ConsumerInfo`, so a test cannot hand a
//! pre-configured mock to the generated producer/consumer directly. Instead
//! the test registers the policy mocks up front (the `expect_*` helpers) keyed
//! by `(instance, event)` — or per instance for service discovery, producer
//! builder and provider info; the generated constructors pop them out. Each
//! slot is consumed once.
//!
//! Event-level slots are typed by the `CommData` type `T` (which implies
//! `'static` via `Reloc`); instance-level slots store the shared non-generic
//! policy handles. All clones of a [`MockRegistry`] share the same map.

use crate::producer_builder::MockProducerBuilderPolicy;
use crate::provider_info::MockProviderInfoPolicy;
use crate::publisher::MockPublisherPolicy;
use crate::service_discovery::MockServiceDiscoveryPolicy;
use crate::subscriber::{MockSubscriberPolicy, MockSubscriptionPolicy};
use core::fmt::{self, Debug};
use score_com_concept::{CommData, InstanceSpecifier};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Which pre-registered mock a slot holds.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum SlotKind {
    Publisher,
    Subscriber,
    Subscription,
    ServiceDiscovery,
    ProducerBuilder,
    ProviderInfo,
}

/// Key of one pre-registered mock. `event` is `""` for instance-level slots.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct SlotKey {
    instance: String,
    event: String,
    kind: SlotKind,
}

impl SlotKey {
    fn new(instance: &InstanceSpecifier, event: &str, kind: SlotKind) -> Self {
        Self {
            instance: instance.as_ref().to_owned(),
            event: event.to_owned(),
            kind,
        }
    }
}

/// Shared store of pre-configured mocks. Cloning shares the same map.
#[derive(Clone, Default)]
pub struct MockRegistry(Arc<Mutex<HashMap<SlotKey, Box<dyn Any + Send>>>>);

impl MockRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a value in a slot, replacing any previous value.
    pub(crate) fn put<V: Any + Send>(&self, key: SlotKey, value: V) {
        self.0
            .lock()
            .expect("poisoned")
            .insert(key, Box::new(value));
    }

    /// Pop a typed value out of a slot; `None` on a miss or a type mismatch.
    pub(crate) fn take<V: Any + Send>(&self, key: &SlotKey) -> Option<V> {
        self.0
            .lock()
            .expect("poisoned")
            .remove(key)
            .and_then(|boxed| boxed.downcast::<V>().ok().map(|b| *b))
    }
}

impl Debug for MockRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.0.lock().expect("poisoned");
        f.debug_tuple("MockRegistry")
            .field(&guard.keys().cloned().collect::<Vec<_>>())
            .finish()
    }
}

/// `Runtime::ConsumerInfo` — `Send + Clone` data threaded by `interface!` into
/// every `Subscriber::new`. Carries the instance + the shared registry so the
/// generated `Subscriber::new` can look up its pre-registered policy.
#[derive(Clone, Debug)]
pub struct MockConsumerInfo {
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: MockRegistry,
}

impl MockConsumerInfo {
    pub(crate) fn new(instance: InstanceSpecifier, registry: MockRegistry) -> Self {
        Self { instance, registry }
    }
}

/// Pre-registered mocks for the `R` endpoints the test has to hand in.
impl MockRegistry {
    /// Register the publisher policy taken by the next `Publisher::new` for
    /// `(instance, event)`.
    pub fn expect_publisher<T: CommData + Debug + 'static>(
        &self,
        instance: &InstanceSpecifier,
        event: &str,
        policy: MockPublisherPolicy<T>,
    ) {
        self.put(
            SlotKey::new(instance, event, SlotKind::Publisher),
            Arc::new(Mutex::new(policy)),
        );
    }

    /// Register the subscriber policy taken by the next `Subscriber::new` for
    /// `(instance, event)`.
    pub fn expect_subscriber<T: CommData + Debug + 'static>(
        &self,
        instance: &InstanceSpecifier,
        event: &str,
        policy: MockSubscriberPolicy<T>,
    ) {
        self.put(
            SlotKey::new(instance, event, SlotKind::Subscriber),
            Arc::new(Mutex::new(policy)),
        );
    }

    /// Register the subscription policy handed to the subscription returned by
    /// the next `Subscriber::subscribe` for `(instance, event)`.
    pub fn expect_subscription<T: CommData + Debug + 'static>(
        &self,
        instance: &InstanceSpecifier,
        event: &str,
        policy: MockSubscriptionPolicy<T>,
    ) {
        self.put(
            SlotKey::new(instance, event, SlotKind::Subscription),
            Arc::new(Mutex::new(policy)),
        );
    }

    /// Register the service discovery policy returned by the next
    /// `find_service` for `instance`.
    pub fn expect_service_discovery(
        &self,
        instance: &InstanceSpecifier,
        policy: MockServiceDiscoveryPolicy,
    ) {
        self.put(
            SlotKey::new(instance, "", SlotKind::ServiceDiscovery),
            Arc::new(Mutex::new(policy)),
        );
    }

    /// Register the producer builder policy returned by the next
    /// `producer_builder` for `instance`.
    pub fn expect_producer_builder(
        &self,
        instance: &InstanceSpecifier,
        policy: MockProducerBuilderPolicy,
    ) {
        self.put(
            SlotKey::new(instance, "", SlotKind::ProducerBuilder),
            Arc::new(Mutex::new(policy)),
        );
    }

    /// Register the provider info policy used by producers of `instance` for
    /// `offer_service` / `stop_offer_service`.
    pub fn expect_provider_info(
        &self,
        instance: &InstanceSpecifier,
        policy: MockProviderInfoPolicy,
    ) {
        self.put(
            SlotKey::new(instance, "", SlotKind::ProviderInfo),
            Arc::new(Mutex::new(policy)),
        );
    }
}

/// Take-side of the registration helpers, used by the generated
/// constructors and the runtime.
pub(crate) mod lookup {
    use super::*;

    pub(crate) fn publisher<T: CommData + Debug + 'static>(
        registry: &MockRegistry,
        instance: &InstanceSpecifier,
        event: &str,
    ) -> Option<Arc<Mutex<MockPublisherPolicy<T>>>> {
        registry.take(&SlotKey::new(instance, event, SlotKind::Publisher))
    }

    pub(crate) fn subscriber<T: CommData + Debug + 'static>(
        registry: &MockRegistry,
        instance: &InstanceSpecifier,
        event: &str,
    ) -> Option<Arc<Mutex<MockSubscriberPolicy<T>>>> {
        registry.take(&SlotKey::new(instance, event, SlotKind::Subscriber))
    }

    pub(crate) fn subscription<T: CommData + Debug + 'static>(
        registry: &MockRegistry,
        instance: &InstanceSpecifier,
        event: &str,
    ) -> Option<Arc<Mutex<MockSubscriptionPolicy<T>>>> {
        registry.take(&SlotKey::new(instance, event, SlotKind::Subscription))
    }

    pub(crate) fn service_discovery(
        registry: &MockRegistry,
        instance: &InstanceSpecifier,
    ) -> Option<Arc<Mutex<MockServiceDiscoveryPolicy>>> {
        registry.take(&SlotKey::new(instance, "", SlotKind::ServiceDiscovery))
    }

    pub(crate) fn producer_builder(
        registry: &MockRegistry,
        instance: &InstanceSpecifier,
    ) -> Option<Arc<Mutex<MockProducerBuilderPolicy>>> {
        registry.take(&SlotKey::new(instance, "", SlotKind::ProducerBuilder))
    }

    pub(crate) fn provider_info(
        registry: &MockRegistry,
        instance: &InstanceSpecifier,
    ) -> Option<Arc<Mutex<MockProviderInfoPolicy>>> {
        registry.take(&SlotKey::new(instance, "", SlotKind::ProviderInfo))
    }
}
