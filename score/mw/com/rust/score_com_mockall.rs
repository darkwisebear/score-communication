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

//! Test-only re-export of the full COM API including the mockall runtime.
//!
//! Depend on `//score/mw/com/rust:score_com_mockall` (instead of `:score_com`).
//!
//! This is the Plan B re-export shim: application code written against
//! `score_com` (including `interface!`-generated code) compiles unmodified
//! against the mockall runtime, which does nothing unless a test scripts it
//! through `expect_*()` on the policy mocks (see the crate docs of
//! `com_api_runtime_mockall` for the full scripting model).

pub use com_api_runtime_mockall::*;
pub use score_com::*;
