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

//! `Runtime::ProviderInfo` handle. Carries the registry into `Publisher::new`.
//!
//! `offer_service` / `stop_offer_service` currently succeed unconditionally.
//! Scriptable offer behavior is guide §3 (ProviderInfoPolicy) and is out of
//! scope for the Publisher GAT slice.

use crate::registry::MockRegistry;
use score_com_concept::{InstanceSpecifier, ProviderInfo, Result};

/// `Runtime::ProviderInfo` — `Send + Clone`; clones share the same registry.
#[derive(Clone, Debug)]
pub struct ProviderInfoHandle {
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: MockRegistry,
}

impl ProviderInfoHandle {
    pub fn new(instance: InstanceSpecifier, registry: MockRegistry) -> Self {
        Self { instance, registry }
    }
}

impl ProviderInfo for ProviderInfoHandle {
    fn offer_service(&self) -> Result<()> {
        Ok(())
    }

    fn stop_offer_service(&self) -> Result<()> {
        Ok(())
    }
}
