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

//! `ProducerBuilder` mock. The `build()` decision is scripted through the
//! non-generic [`ProducerBuilderPolicy`] (see [`crate::service_discovery`] for
//! why the policy is not generic over `I`); the constructed producer carries
//! the instance's [`ProviderInfoHandle`] whose `offer_service` /
//! `stop_offer_service` are separately scripted.

use crate::provider_info::handle_from_registry;
use crate::registry::MockRegistry;
use crate::runtime::MockRuntimeImpl;
use core::marker::PhantomData;
use mockall::automock;
use score_com_concept::{Builder, InstanceSpecifier, Interface, Producer, ProducerBuilder, Result};
use std::sync::{Arc, Mutex};

/// Scriptable decisions of one producer builder.
#[automock]
pub trait ProducerBuilderPolicy: Send + 'static {
    /// Called by `build()`; assert call counts and/or inject a failure.
    fn build(&self) -> Result<()>;
}

/// `ProducerBuilder` for offered services.
pub struct MockProducerBuilderImpl<I: Interface> {
    instance: InstanceSpecifier,
    registry: MockRegistry,
    policy: Arc<Mutex<MockProducerBuilderPolicy>>,
    _i: PhantomData<I>,
}

impl<I: Interface> MockProducerBuilderImpl<I> {
    /// Test-only constructor with a fresh, unconfigured policy.
    pub fn new(instance: InstanceSpecifier, registry: MockRegistry) -> Self {
        Self {
            instance,
            registry,
            policy: Arc::new(Mutex::new(MockProducerBuilderPolicy::new())),
            _i: PhantomData,
        }
    }

    /// Test-only constructor wrapping an already-scripted policy.
    pub fn from_policy(
        instance: InstanceSpecifier,
        registry: MockRegistry,
        policy: MockProducerBuilderPolicy,
    ) -> Self {
        Self::from_shared(instance, registry, Arc::new(Mutex::new(policy)))
    }

    /// Wrap an already-`Arc`'d shared policy (used by the registry lookup and
    /// so that cloned builders share expectations).
    pub(crate) fn from_shared(
        instance: InstanceSpecifier,
        registry: MockRegistry,
        policy: Arc<Mutex<MockProducerBuilderPolicy>>,
    ) -> Self {
        Self {
            instance,
            registry,
            policy,
            _i: PhantomData,
        }
    }
}

impl<I: Interface> Builder<I::Producer<MockRuntimeImpl>> for MockProducerBuilderImpl<I> {
    fn build(self) -> Result<I::Producer<MockRuntimeImpl>> {
        self.policy.lock().expect("poisoned").build()?;
        let provider_info = handle_from_registry(self.instance, self.registry);
        // `Producer::new` already returns `Result<Self>`; `Self` is inferred to
        // `I::Producer<MockRuntimeImpl>` from the return type.
        Producer::new(provider_info)
    }
}

impl<I: Interface> ProducerBuilder<I, MockRuntimeImpl> for MockProducerBuilderImpl<I> {}
