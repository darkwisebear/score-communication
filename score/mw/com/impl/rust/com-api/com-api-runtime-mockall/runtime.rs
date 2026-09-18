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

//! `MockRuntimeImpl` — GAT slots point at mock / stub types.
//!
//! Only `Publisher` and `ProviderInfo` / `ConsumerInfo` are real in this slice.
//! Other associated types come from [`crate::stubs`] and will be replaced later.

use crate::provider_info::ProviderInfoHandle;
use crate::publisher::MockPublisherImpl;
use crate::registry::{MockConsumerInfo, MockRegistry};
use crate::stubs::{StubProducerBuilder, StubServiceDiscovery, StubSubscriber};
use core::fmt::Debug;
use score_com_concept::{
    Builder, CommData, FindServiceSpecifier, InstanceSpecifier, Interface, Result, Runtime,
    RuntimeBuilder,
};
use std::path::Path;

/// Classical mockall runtime. Cloning shares the same [`MockRegistry`].
#[derive(Clone, Default)]
pub struct MockRuntimeImpl {
    registry: MockRegistry,
}

impl MockRuntimeImpl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn registry(&self) -> &MockRegistry {
        &self.registry
    }
}

impl Runtime for MockRuntimeImpl {
    type ServiceDiscovery<I: Interface + Send> = StubServiceDiscovery<I>;
    type Subscriber<T: CommData + Debug> = StubSubscriber<T>;
    type ProducerBuilder<I: Interface> = StubProducerBuilder<I>;
    type Publisher<T: CommData + Debug> = MockPublisherImpl<T>;
    type ProviderInfo = ProviderInfoHandle;
    type ConsumerInfo = MockConsumerInfo;

    fn find_service<I: Interface + Send>(
        &self,
        _instance_specifier: FindServiceSpecifier,
    ) -> Self::ServiceDiscovery<I> {
        StubServiceDiscovery::new()
    }

    fn producer_builder<I: Interface>(
        &self,
        _instance_specifier: InstanceSpecifier,
    ) -> Self::ProducerBuilder<I> {
        StubProducerBuilder::new()
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
