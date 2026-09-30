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

//! Producer-side builder for the fake backend.
//!
//! [`FakeProducerBuilder`] is returned by
//! [`FakeRuntimeImpl::producer_builder`](crate::runtime::FakeRuntimeImpl) and
//! builds the `interface!`-generated `I::Producer` type through the
//! [`score_com_concept::Producer`] trait; there is nothing else to configure
//! in an in-memory backend.

use crate::provider_info::FakeProviderInfo;
use crate::registry::FakeRegistry;
use crate::runtime::FakeRuntimeImpl;
use core::marker::PhantomData;
use score_com_concept::{Builder, InstanceSpecifier, Interface, Result};
use std::fmt::Debug;

/// Builder of a producer instance for one interface.
pub struct FakeProducerBuilder<I: Interface> {
    pub(crate) interface_id: &'static str,
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: FakeRegistry,
    pub(crate) _i: PhantomData<I>,
}

impl<I: Interface> Clone for FakeProducerBuilder<I> {
    fn clone(&self) -> Self {
        Self {
            interface_id: self.interface_id,
            instance: self.instance.clone(),
            registry: self.registry.clone(),
            _i: PhantomData,
        }
    }
}

impl<I: Interface> Debug for FakeProducerBuilder<I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FakeProducerBuilder")
            .field("interface_id", &self.interface_id)
            .field("instance", &self.instance)
            .finish()
    }
}

impl<I: Interface> Builder<I::Producer<FakeRuntimeImpl>> for FakeProducerBuilder<I> {
    fn build(self) -> Result<I::Producer<FakeRuntimeImpl>> {
        let info = FakeProviderInfo::new(
            self.interface_id,
            self.instance.clone(),
            self.registry.clone(),
        );
        // `Producer::new` returns a `Result`.
        <I::Producer<FakeRuntimeImpl> as
            score_com_concept::Producer<FakeRuntimeImpl>>::new(info)
    }
}

impl<I: Interface> score_com_concept::ProducerBuilder<I, FakeRuntimeImpl>
    for FakeProducerBuilder<I>
{
}
