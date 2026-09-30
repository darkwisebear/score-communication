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

//! `Runtime` implementation for the functional fake backend (Plan A).
//!
//! All six `Runtime` associated types point at the hand-written fake types in
//! this crate. The runtime owns a [`FakeRegistry`] (cloning shares the same
//! registry), so producers, consumers, publishers and subscriptions created
//! through any clone exchange real data through the shared in-memory event
//! queues.

use crate::consumer::{FakeConsumerDiscovery, FakeConsumerInfo};
use crate::producer_builder::FakeProducerBuilder;
use crate::provider_info::FakeProviderInfo;
use crate::publisher::FakePublisher;
use crate::registry::FakeRegistry;
use crate::subscriber::FakeSubscriber;
use core::fmt::Debug;
use core::marker::PhantomData;
use score_com_concept::{
    CommData, FindServiceSpecifier, InstanceSpecifier, Interface, Runtime, RuntimeBuilder,
};
use std::path::Path;

/// Functional fake runtime. Cloning shares the same [`FakeRegistry`], so all
/// handles a runtime hands out interoperate.
#[derive(Clone)]
pub struct FakeRuntimeImpl {
    registry: FakeRegistry,
}

impl FakeRuntimeImpl {
    pub fn new() -> Self {
        Self {
            registry: FakeRegistry::new(),
        }
    }

    /// Test-only constructor sharing an existing registry (e.g. to pre-populate
    /// state across a producer and consumer runtime in one test).
    pub fn with_registry(registry: FakeRegistry) -> Self {
        Self { registry }
    }

    /// The shared registry backing this runtime.
    pub fn registry(&self) -> &FakeRegistry {
        &self.registry
    }
}

impl Default for FakeRuntimeImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for FakeRuntimeImpl {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FakeRuntimeImpl").field("registry", &self.registry).finish()
    }
}

impl Runtime for FakeRuntimeImpl {
    type ServiceDiscovery<I: Interface + Send> = FakeConsumerDiscovery<I>;
    type Subscriber<T: CommData + Debug> = FakeSubscriber<T>;
    type ProducerBuilder<I: Interface> = FakeProducerBuilder<I>;
    type Publisher<T: CommData + Debug> = FakePublisher<T>;
    type ProviderInfo = FakeProviderInfo;
    type ConsumerInfo = FakeConsumerInfo;

    fn find_service<I: Interface + Send>(
        &self,
        instance_specifier: FindServiceSpecifier,
    ) -> Self::ServiceDiscovery<I> {
        // `FindServiceSpecifier` is not `Clone`/`Send`; convert it eagerly to
        // an owned `Option<String>` for the async discovery path.
        let wanted_instance = match instance_specifier {
            FindServiceSpecifier::Specific(spec) => Some(spec.as_ref().to_owned()),
            FindServiceSpecifier::Any => None,
        };
        FakeConsumerDiscovery {
            interface_id: I::INTERFACE_ID,
            registry: self.registry.clone(),
            wanted_instance,
            _i: PhantomData,
        }
    }

    fn producer_builder<I: Interface>(
        &self,
        instance_specifier: InstanceSpecifier,
    ) -> Self::ProducerBuilder<I> {
        FakeProducerBuilder {
            interface_id: I::INTERFACE_ID,
            instance: instance_specifier,
            registry: self.registry.clone(),
            _i: PhantomData,
        }
    }
}

/// No-op builder so the fake runtime is a drop-in for
/// `LolaRuntimeBuilderImpl` (config loading is ignored; the in-memory backend
/// has no configuration).
#[derive(Default)]
pub struct FakeRuntimeBuilderImpl {}

impl FakeRuntimeBuilderImpl {
    pub fn new() -> Self {
        Self::default()
    }
}

impl score_com_concept::Builder<FakeRuntimeImpl> for FakeRuntimeBuilderImpl {
    fn build(self) -> score_com_concept::Result<FakeRuntimeImpl> {
        Ok(FakeRuntimeImpl::new())
    }
}

impl RuntimeBuilder<FakeRuntimeImpl> for FakeRuntimeBuilderImpl {
    fn load_config(&mut self, _config: &Path) -> &mut Self {
        self
    }
}
