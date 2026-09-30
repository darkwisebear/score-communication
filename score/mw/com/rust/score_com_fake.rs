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

//! Test-only re-export of the full COM API including the functional fake
//! (Plan A) runtime.
//!
//! Depend on `//score/mw/com/rust:score_com_fake` (instead of `:score_com`).
//!
//! Application code written against `score_com` (including `interface!`-
//! generated code) compiles unmodified against the fake runtime, where
//! published samples are **actually delivered** to subscriptions over an
//! in-memory pub/sub registry — no backend, config file or expectation
//! scripting involved (see the crate docs of `com_api_runtime_fake`).

pub use com_api_runtime_fake::*;
pub use score_com::*;
