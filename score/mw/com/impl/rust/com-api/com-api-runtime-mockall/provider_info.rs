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

//! `Runtime::ProviderInfo` handle.
//!
//! [`ProviderInfoHandle`] carries the instance + the shared registry into
//! `Publisher::new` (called internally by `interface!`'s `offer()`). The
//! `offer_service` / `stop_offer_service` calls are scripted through
//! [`ProviderInfoPolicy`]; the policy is shared (`Arc<Mutex<_>>`) because the
//! macro clones the handle once per event plus once for the offered producer.
//! Mirrors `SharedMockBridge` in `com-api-ffi-lola`.

use crate::registry::{lookup, MockRegistry};
use mockall::automock;
use score_com_concept::{InstanceSpecifier, ProviderInfo, Result};
use std::sync::{Arc, Mutex};

/// Scriptable decisions of [`ProviderInfoHandle`].
#[automock]
pub trait ProviderInfoPolicy: Send + 'static {
    /// Called by `Producer::offer`; inject an offer failure here.
    fn offer_service(&self) -> Result<()>;
    /// Called by `OfferedProducer::unoffer`; inject an unoffer failure here.
    fn stop_offer_service(&self) -> Result<()>;
}

/// `Runtime::ProviderInfo` — `Send + Clone`; all clones share the same policy
/// expectations and registry. Carries the instance so `Publisher::new` (called
/// by `interface!`'s `offer()`) can look up its pre-registered policy.
#[derive(Clone)]
pub struct ProviderInfoHandle {
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: MockRegistry,
    pub(crate) policy: Arc<Mutex<MockProviderInfoPolicy>>,
}

impl ProviderInfoHandle {
    /// Test-only constructor: skip the registry and wrap an already-scripted
    /// policy.
    pub fn from_policy(
        instance: InstanceSpecifier,
        registry: MockRegistry,
        policy: MockProviderInfoPolicy,
    ) -> Self {
        Self::from_shared(instance, registry, Arc::new(Mutex::new(policy)))
    }

    /// Wrap an already-`Arc`'d shared policy (used by the registry lookup and
    /// so that cloned handles share expectations).
    pub(crate) fn from_shared(
        instance: InstanceSpecifier,
        registry: MockRegistry,
        policy: Arc<Mutex<MockProviderInfoPolicy>>,
    ) -> Self {
        Self {
            instance,
            registry,
            policy,
        }
    }

    /// Post-hoc assertions / late expectations on the shared policy.
    pub fn with_mock<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut MockProviderInfoPolicy) -> R,
    {
        let mut guard = self.policy.lock().expect("poisoned");
        f(&mut guard)
    }
}

impl core::fmt::Debug for ProviderInfoHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ProviderInfoHandle")
            .field("instance", &self.instance)
            .field("registry", &self.registry)
            .finish()
    }
}

impl ProviderInfo for ProviderInfoHandle {
    fn offer_service(&self) -> Result<()> {
        self.policy.lock().expect("poisoned").offer_service()
    }

    fn stop_offer_service(&self) -> Result<()> {
        self.policy.lock().expect("poisoned").stop_offer_service()
    }
}

/// Internal: build the handle from the registry (policy popped, fresh
/// unconfigured mock on a miss — the first unexpected call then panics with a
/// clear mockall message).
pub(crate) fn handle_from_registry(
    instance: InstanceSpecifier,
    registry: MockRegistry,
) -> ProviderInfoHandle {
    let policy = lookup::provider_info(&registry, &instance)
        .unwrap_or_else(|| Arc::new(Mutex::new(MockProviderInfoPolicy::new())));
    ProviderInfoHandle::from_shared(instance, registry, policy)
}

#[cfg(test)]
mod test {
    use super::*;

    fn spec() -> InstanceSpecifier {
        InstanceSpecifier::new("/Test/Service1/Instance").expect("valid instance specifier")
    }

    #[test]
    fn provider_info_offer_is_scripted_and_shared_by_clones() {
        let mut policy = MockProviderInfoPolicy::new();
        policy
            .expect_offer_service()
            .times(2)
            .returning(|| Ok(()));
        policy
            .expect_stop_offer_service()
            .times(1)
            .returning(|| Ok(()));

        // `interface!` clones the handle once per event + once for the offered
        // producer, so the clones must share these expectations.
        let handle = ProviderInfoHandle::from_policy(spec(), MockRegistry::new(), policy);
        let clone = handle.clone();
        assert!(handle.offer_service().is_ok());
        assert!(clone.offer_service().is_ok());
        assert!(handle.stop_offer_service().is_ok());
        // mockall verifies the `times(..)` counts on drop.
    }

    #[test]
    fn handle_from_registry_pops_registered_policy() {
        let mut policy = MockProviderInfoPolicy::new();
        policy
            .expect_offer_service()
            .times(1)
            .returning(|| Ok(()));
        let registry = MockRegistry::new();
        registry.expect_provider_info(&spec(), policy);
        let handle = handle_from_registry(spec(), registry);
        assert!(handle.offer_service().is_ok());
    }
}
