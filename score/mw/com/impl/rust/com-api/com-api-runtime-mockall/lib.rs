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

//! Classical mockall backend for `score_com_concept::Runtime` (Plan B).
//!
//! Nothing happens unless a test scripts it through `expect_*()` on policy mocks.
//! Generic associated types cannot be `#[automock]`-ed; those traits use a
//! lifetime-free **policy** plus a hand-written wrapper. The Publisher
//! workaround in [`publisher`] is the blueprint for later GAT traits
//! (`Subscription`).
//!
//! Temporary `stubs` cover the remaining `Runtime` associated types until their
//! own slices land.

// Lifetime parameters on sample types are required by the Publisher GAT even
// when the inner fields own their data.
#![allow(clippy::needless_lifetimes)]

mod provider_info;
mod publisher;
mod registry;
mod runtime;
mod stubs;

pub use provider_info::ProviderInfoHandle;
pub use publisher::{
    MockPublisherImpl, MockPublisherPolicy, MockSampleMaybeUninit, MockSampleMut, PublisherPolicy,
};
pub use registry::{MockConsumerInfo, MockRegistry};
pub use runtime::{MockRuntimeBuilderImpl, MockRuntimeImpl};
