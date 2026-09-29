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

//! `ServiceDiscovery` + `ConsumerBuilder` mocks. No GATs here, so the
//! scriptable decisions go on plain `#[automock]`-ed policy traits; the
//! wrappers only adapt them to the `ServiceDiscovery` trait (associated types
//! + the async signature) and build `I::Consumer` through the `Consumer`
//! trait.
//!
//! The policies are deliberately **not generic over `I`** (and
//! [`SharedConsumerBuilder`] is a plain data struct, not a mock):
//! `Interface` carries no `'static` bound, so mockall's generated
//! `Send + 'static` mock structs would not be nameable as
//! `I::Consumer<R>` GAT projections. Sharing goes through the `Arc<Mutex<_>>`
//! policy handles instead; the typed wrappers are zero-cost
//! (`PhantomData<I>`) views over the shared state.

use crate::registry::MockRegistry;
use crate::runtime::MockRuntimeImpl;
use core::future::Future;
use core::marker::PhantomData;
use mockall::automock;
use score_com_concept::{
    Builder, Consumer, ConsumerBuilder, ConsumerDescriptor, InstanceSpecifier, Interface, Result,
    ServiceDiscovery,
};
use std::sync::{Arc, Mutex};

/// Shared, cloneable expectations of one consumer builder.
///
/// Cloning shares the same policy expectations and registry; it is the value
/// the service discovery mock returns from `builders()`.
#[derive(Clone)]
pub struct SharedConsumerBuilder {
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: MockRegistry,
    pub(crate) policy: Arc<Mutex<MockConsumerBuilderPolicy>>,
}

impl SharedConsumerBuilder {
    /// Test-only constructor with a fresh, unconfigured policy.
    pub fn new(instance: InstanceSpecifier, registry: MockRegistry) -> Self {
        Self {
            instance,
            registry,
            policy: Arc::new(Mutex::new(MockConsumerBuilderPolicy::new())),
        }
    }

    /// Test-only constructor wrapping an already-scripted policy.
    pub fn from_policy(
        instance: InstanceSpecifier,
        registry: MockRegistry,
        policy: MockConsumerBuilderPolicy,
    ) -> Self {
        Self {
            instance,
            registry,
            policy: Arc::new(Mutex::new(policy)),
        }
    }
}

impl core::fmt::Debug for SharedConsumerBuilder {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SharedConsumerBuilder")
            .field("instance", &self.instance)
            .field("registry", &self.registry)
            .finish()
    }
}

/// Scriptable decisions of one service discovery handle.
#[automock]
pub trait ServiceDiscoveryPolicy: Send + 'static {
    /// Called by `get_available_instances` / `get_available_instances_async`;
    /// must return the discovered consumer builders for this interface.
    fn builders(&self) -> Result<Vec<SharedConsumerBuilder>>;
}

/// Hand-written `ServiceDiscovery` wrapper; clones of the same wrapper share
/// the same policy expectations.
pub struct MockServiceDiscoveryImpl<I: Interface + Send> {
    policy: Arc<Mutex<MockServiceDiscoveryPolicy>>,
    _i: PhantomData<I>,
}

impl<I: Interface + Send> MockServiceDiscoveryImpl<I> {
    /// Test-only constructor: skip the registry and wrap an already-scripted
    /// policy.
    pub fn from_policy(policy: MockServiceDiscoveryPolicy) -> Self {
        Self::from_shared(Arc::new(Mutex::new(policy)))
    }

    /// Wrap an already-`Arc`'d shared policy (used by the registry lookup and
    /// so that cloned discovery handles share expectations).
    pub(crate) fn from_shared(policy: Arc<Mutex<MockServiceDiscoveryPolicy>>) -> Self {
        Self {
            policy,
            _i: PhantomData,
        }
    }

    /// Shared handle to the discovery's expectations.
    pub fn with_mock<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut MockServiceDiscoveryPolicy) -> R,
    {
        let mut guard = self.policy.lock().expect("poisoned");
        f(&mut guard)
    }
}

impl<I: Interface + Send> ServiceDiscovery<I, MockRuntimeImpl> for MockServiceDiscoveryImpl<I> {
    type ConsumerBuilder = MockConsumerBuilderImpl<I>;
    type ServiceEnumerator = Vec<MockConsumerBuilderImpl<I>>;

    fn get_available_instances(&self) -> Result<Self::ServiceEnumerator> {
        Ok(self
            .policy
            .lock()
            .expect("poisoned")
            .builders()?
            .into_iter()
            .map(MockConsumerBuilderImpl::from_shared)
            .collect())
    }

    #[allow(clippy::manual_async_fn)]
    fn get_available_instances_async(
        &self,
    ) -> impl Future<Output = Result<Self::ServiceEnumerator>> + Send {
        let policy = Arc::clone(&self.policy);
        async move {
            Ok(policy
                .lock()
                .expect("poisoned")
                .builders()?
                .into_iter()
                .map(MockConsumerBuilderImpl::from_shared)
                .collect())
        }
    }
}

/// Scriptable decisions of one consumer builder.
#[automock]
pub trait ConsumerBuilderPolicy: Send + 'static {
    /// Called by `build()`; assert call counts and/or inject a failure.
    fn build(&self) -> Result<()>;
}

/// `ConsumerBuilder` for discovered services; a typed view over
/// [`SharedConsumerBuilder`]. Carries the instance (and the shared registry)
/// into `I::Consumer::new`; `build()` is scripted.
pub struct MockConsumerBuilderImpl<I: Interface + Send> {
    shared: SharedConsumerBuilder,
    _i: PhantomData<I>,
}

impl<I: Interface + Send> MockConsumerBuilderImpl<I> {
    /// Test-only constructor with a fresh, unconfigured policy.
    pub fn new(instance: InstanceSpecifier, registry: MockRegistry) -> Self {
        Self {
            shared: SharedConsumerBuilder::new(instance, registry),
            _i: PhantomData,
        }
    }

    /// Test-only constructor wrapping an already-scripted policy.
    pub fn from_policy(
        instance: InstanceSpecifier,
        registry: MockRegistry,
        policy: MockConsumerBuilderPolicy,
    ) -> Self {
        Self {
            shared: SharedConsumerBuilder::from_policy(instance, registry, policy),
            _i: PhantomData,
        }
    }

    fn from_shared(shared: SharedConsumerBuilder) -> Self {
        Self {
            shared,
            _i: PhantomData,
        }
    }
}

impl<I: Interface + Send> ConsumerDescriptor<MockRuntimeImpl> for MockConsumerBuilderImpl<I> {
    fn get_instance_identifier(&self) -> &InstanceSpecifier {
        &self.shared.instance
    }
}

impl<I: Interface + Send> Builder<I::Consumer<MockRuntimeImpl>> for MockConsumerBuilderImpl<I> {
    fn build(self) -> Result<I::Consumer<MockRuntimeImpl>> {
        self.shared.policy.lock().expect("poisoned").build()?;
        // `Consumer::new` returns `Self` directly (not a `Result`).
        let info = crate::registry::MockConsumerInfo::new(
            self.shared.instance,
            self.shared.registry,
        );
        Ok(Consumer::new(info))
    }
}

impl<I: Interface + Send> ConsumerBuilder<I, MockRuntimeImpl> for MockConsumerBuilderImpl<I> {}
