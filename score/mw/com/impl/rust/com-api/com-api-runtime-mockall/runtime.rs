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

//! `MockRuntimeImpl` — a classical mockall runtime.
//!
//! All six `Runtime` associated types are populated by mock-backed types.
//! `find_service` / `producer_builder` return the mock pre-registered in the
//! shared [`MockRegistry`] (or a fresh, unconfigured one that panics with a
//! clear mockall message on first use). Everything the test can observe is
//! reachable through an `expect_*()` call on a policy mock; nothing happens
//! unless it is scripted.

use crate::producer_builder::MockProducerBuilderImpl;
use crate::provider_info::ProviderInfoHandle;
use crate::publisher::MockPublisherImpl;
use crate::registry::{lookup, MockConsumerInfo, MockRegistry};
use crate::service_discovery::MockServiceDiscoveryImpl;
use crate::subscriber::MockSubscriberImpl;
use core::fmt::Debug;
use score_com_concept::{
    Builder, CommData, FindServiceSpecifier, InstanceSpecifier, Interface, Result, Runtime,
    RuntimeBuilder,
};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Classical mockall runtime. Cloning shares the same [`MockRegistry`], so all
/// handles a runtime hands out script the same expectations.
#[derive(Clone, Default)]
pub struct MockRuntimeImpl {
    registry: MockRegistry,
}

impl MockRuntimeImpl {
    pub fn new() -> Self {
        Self::default()
    }

    /// The shared store of pre-configured mocks; use its `expect_*` helpers to
    /// register policies before offering/finding a service.
    pub fn registry(&self) -> &MockRegistry {
        &self.registry
    }

    /// Pre-load the provider-info policy (`offer_service`/`stop_offer_service`)
    /// used by producers of `instance`.
    pub fn expect_provider_info(
        &self,
        instance: &InstanceSpecifier,
        policy: crate::provider_info::MockProviderInfoPolicy,
    ) {
        self.registry.expect_provider_info(instance, policy);
    }

    /// Pre-load the producer-builder policy returned by the next
    /// `producer_builder` for `instance`.
    pub fn expect_producer_builder(
        &self,
        instance: &InstanceSpecifier,
        policy: crate::producer_builder::MockProducerBuilderPolicy,
    ) {
        self.registry.expect_producer_builder(instance, policy);
    }

    /// Pre-load the service-discovery policy returned by the next
    /// `find_service` for `instance`.
    pub fn expect_service_discovery(
        &self,
        instance: &InstanceSpecifier,
        policy: crate::service_discovery::MockServiceDiscoveryPolicy,
    ) {
        self.registry.expect_service_discovery(instance, policy);
    }
}

impl Runtime for MockRuntimeImpl {
    type ServiceDiscovery<I: Interface + Send> = MockServiceDiscoveryImpl<I>;
    type Subscriber<T: CommData + Debug> = MockSubscriberImpl<T>;
    type ProducerBuilder<I: Interface> = MockProducerBuilderImpl<I>;
    type Publisher<T: CommData + Debug> = MockPublisherImpl<T>;
    type ProviderInfo = ProviderInfoHandle;
    type ConsumerInfo = MockConsumerInfo;

    fn find_service<I: Interface + Send>(
        &self,
        instance_specifier: FindServiceSpecifier,
    ) -> Self::ServiceDiscovery<I> {
        let instance = match instance_specifier {
            FindServiceSpecifier::Specific(spec) => spec,
            FindServiceSpecifier::Any => InstanceSpecifier::new("*").expect("valid specifier"),
        };
        // Pre-registered mock, or a fresh unconfigured one that panics on use.
        let policy = lookup::service_discovery(&self.registry, &instance).unwrap_or_else(|| {
            Arc::new(Mutex::new(
                crate::service_discovery::MockServiceDiscoveryPolicy::new(),
            ))
        });
        MockServiceDiscoveryImpl::from_shared(policy)
    }

    fn producer_builder<I: Interface>(
        &self,
        instance_specifier: InstanceSpecifier,
    ) -> Self::ProducerBuilder<I> {
        // Pre-registered builder, or a fresh unconfigured one whose scripted
        // `build()` panics on use.
        let policy =
            lookup::producer_builder(&self.registry, &instance_specifier).unwrap_or_else(|| {
                Arc::new(Mutex::new(
                    crate::producer_builder::MockProducerBuilderPolicy::new(),
                ))
            });
        MockProducerBuilderImpl::from_shared(instance_specifier, self.registry.clone(), policy)
    }
}

/// No-op builder so the mock runtime is a drop-in for `LolaRuntimeBuilderImpl`.
#[derive(Default)]
pub struct MockRuntimeBuilderImpl {}

impl MockRuntimeBuilderImpl {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Builder<MockRuntimeImpl> for MockRuntimeBuilderImpl {
    fn build(self) -> Result<MockRuntimeImpl> {
        Ok(MockRuntimeImpl::new())
    }
}

impl RuntimeBuilder<MockRuntimeImpl> for MockRuntimeBuilderImpl {
    fn load_config(&mut self, _config: &Path) -> &mut Self {
        self
    }
}
