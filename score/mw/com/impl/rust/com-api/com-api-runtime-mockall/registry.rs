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
//! `interface!` constructs `Publisher`/`Subscriber` internally, so tests register
//! policies here keyed by `(instance, event, kind)` before offering/finding a
//! service. Each slot is consumed once (`take`).

use crate::publisher::MockPublisherPolicy;
use core::fmt::{self, Debug};
use score_com_concept::{CommData, InstanceSpecifier};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct SlotKey {
    instance: String,
    event: String,
    kind: SlotKind,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[allow(dead_code)] // other kinds land in later Plan B slices
pub(crate) enum SlotKind {
    Subscriber,
    Subscription,
    Publisher,
    ServiceDiscovery,
    ProducerBuilder,
}

/// Shared store of pre-configured mocks. Cloning shares the same map.
#[derive(Clone, Default)]
pub struct MockRegistry(Arc<Mutex<HashMap<SlotKey, Box<dyn Any + Send>>>>);

impl MockRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn put<V: Any + Send>(&self, key: SlotKey, value: V) {
        self.0
            .lock()
            .expect("poisoned")
            .insert(key, Box::new(value));
    }

    pub(crate) fn take<V: Any + Send>(&self, key: &SlotKey) -> Option<V> {
        self.0
            .lock()
            .expect("poisoned")
            .remove(key)
            .and_then(|boxed| boxed.downcast::<V>().ok().map(|b| *b))
    }

    /// Register the policy that the next `Publisher::new` for this event shall take.
    pub fn expect_publisher<T: CommData + Debug + 'static>(
        &self,
        instance: &InstanceSpecifier,
        event: &str,
        policy: MockPublisherPolicy<T>,
    ) {
        self.put(
            SlotKey {
                instance: instance.as_ref().to_owned(),
                event: event.to_owned(),
                kind: SlotKind::Publisher,
            },
            policy,
        );
    }

    pub(crate) fn take_publisher<T: CommData + Debug + 'static>(
        &self,
        instance: &InstanceSpecifier,
        event: &str,
    ) -> Option<MockPublisherPolicy<T>> {
        self.take(&SlotKey {
            instance: instance.as_ref().to_owned(),
            event: event.to_owned(),
            kind: SlotKind::Publisher,
        })
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

/// `Runtime::ConsumerInfo` — `Send + Clone` data, no trait to mock.
#[derive(Clone, Debug)]
#[allow(dead_code)] // fields are read once Subscriber::new lands
pub struct MockConsumerInfo {
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: MockRegistry,
}
