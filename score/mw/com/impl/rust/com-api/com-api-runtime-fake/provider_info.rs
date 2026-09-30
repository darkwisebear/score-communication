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

//! `Runtime::ProviderInfo` for the functional fake backend.
//!
//! [`FakeProviderInfo`] carries the `(interface, instance, registry)`
//! coordinates and makes offering a producer a **real** operation:
//! `offer_service` registers the instance in the shared registry (so it
//! becomes discoverable) and `stop_offer_service` withdraws it. Cloning shares
//! the same registry, matching how `interface!` clones the handle once per
//! event plus once for the offered producer.
//!
//! Per the Plan A groundwork, this crate is mockall-free: the default
//! behavior "just works" with no expectation scripting.

use crate::registry::FakeRegistry;
use score_com_concept::{InstanceSpecifier, ProviderInfo, Result};
use std::fmt::Debug;

/// `Runtime::ProviderInfo` — `Send + Clone`.
#[derive(Clone)]
pub struct FakeProviderInfo {
    pub(crate) interface_id: String,
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: FakeRegistry,
}

impl FakeProviderInfo {
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

    /// The interface this provider instance belongs to.
    pub fn interface_id(&self) -> &str {
        &self.interface_id
    }

    /// The instance specifier of this provider.
    pub fn instance(&self) -> &InstanceSpecifier {
        &self.instance
    }

    /// The registry this provider operates on.
    pub fn registry(&self) -> &FakeRegistry {
        &self.registry
    }
}

impl Debug for FakeProviderInfo {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FakeProviderInfo")
            .field("interface_id", &self.interface_id)
            .field("instance", &self.instance)
            .finish()
    }
}

impl ProviderInfo for FakeProviderInfo {
    fn offer_service(&self) -> Result<()> {
        // Real behavior: register the instance as offered. This also wakes any
        // parked async service discovery of the same interface.
        self.registry
            .set_offered(&self.interface_id, &self.instance, true);
        Ok(())
    }

    fn stop_offer_service(&self) -> Result<()> {
        // Real behavior: withdraw the instance from discovery.
        self.registry
            .set_offered(&self.interface_id, &self.instance, false);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn spec() -> InstanceSpecifier {
        InstanceSpecifier::new("/Test/Service1/Instance").expect("valid instance specifier")
    }

    #[test]
    fn offer_makes_instance_discoverable() {
        let registry = FakeRegistry::new();
        let info = FakeProviderInfo::new("Test", spec(), registry.clone());
        assert!(!registry.is_offered("Test", info.instance()));

        info.offer_service().expect("offer");
        assert!(registry.is_offered("Test", info.instance()));
        // `InstanceSpecifier` is not `PartialEq`, so compare by string form.
        let offered = registry.offered_instances("Test");
        assert_eq!(offered.len(), 1);
        assert_eq!(offered[0].as_ref(), info.instance().as_ref());

        info.stop_offer_service().expect("unoffer");
        assert!(!registry.is_offered("Test", info.instance()));
        assert!(registry.offered_instances("Test").is_empty());
    }

    #[test]
    fn clones_share_registry() {
        let registry = FakeRegistry::new();
        let info = FakeProviderInfo::new("Test", spec(), registry.clone());
        let clone = info.clone();
        clone.offer_service().expect("offer via clone");
        // The original handle sees the offer on the shared registry.
        assert!(registry.is_offered("Test", info.instance()));
        info.stop_offer_service().expect("unoffer via original");
        assert!(!registry.is_offered("Test", info.instance()));
    }
}
