# *******************************************************************************
# Copyright (c) 2026 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache License Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0
# *******************************************************************************

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "com-api-runtime-mockall",
    testonly = True,
    srcs = [
        "lib.rs",
        "provider_info.rs",
        "publisher.rs",
        "registry.rs",
        "runtime.rs",
        "stubs.rs",
    ],
    crate_name = "com_api_runtime_mockall",
    visibility = ["//score/mw/com:__subpackages__"],
    deps = [
        "//score/mw/com/rust/score_com_concept",
        "@score_communication_crate_index//:futures",
        "@score_communication_crate_index//:mockall",
    ],
)

rust_test(
    name = "com-api-runtime-mockall-tests",
    crate = ":com-api-runtime-mockall",
    deps = [
        "@score_communication_crate_index//:mockall",
    ],
)
