# AGENTS.md

Repository knowledge for future OpenHands sessions on this repo
(Eclipse `communication`, Bazel + rules_rust, Rust edition 2024).

## Plan B: mockall COM runtime (issue #490)

A test-only runtime that implements `score_com_concept::Runtime` using
`mockall`, so application code (including `interface!`-generated code) runs
unmodified against a runtime that does nothing unless a test scripts it via
`expect_*()` on the `Mock*Policy` mocks.

- Crate: `score/mw/com/impl/rust/com-api/com-api-runtime-mockall`
  (crate name `com_api_runtime_mockall`, `testonly = True`, deps:
  `score_com_concept`, `futures`, `mockall`).
- Re-export shim: `score/mw/com/rust/score_com_mockall.rs` (target
  `//score/mw/com/rust:score_com_mockall`, `testonly = True`,
  `crate_name = "score_com"` — mandatory because `interface!` expands to
  absolute `score_com::…` paths; the `crate` name must stay `score_com`, the
  *target* name is what differs).
- Proof / example test:
  `score/mw/com/example/com-api-example/tests_using_mockall_runtime.rs`
  (target `//score/mw/com/example/com-api-example:com-api-example-mockall-runtime-test`,
  mirrors `tests_using_tokio_runtime.rs`, runs in CI, no config file needed).

## Building / testing

Bazel (via the workspace `bazel` wrapper). Useful targets:

```bash
bazel build //score/mw/com/impl/rust/com-api/com-api-runtime-mockall:com-api-runtime-mockall
bazel test  //score/mw/com/impl/rust/com-api/com-api-runtime-mockall:com-api-runtime-mockall-tests
bazel test  //score/mw/com/example/com-api-example:com-api-example-mockall-runtime-test --test_output=all
```

The Lola-based example test
(`com-api-example-tokio-integration-test`) is tagged `manual` and needs a
config file + backend; the mockall test has no `manual` tag and needs neither.

## Gotchas learned

- `interface!` `offer()` creates a `Publisher` for **every** event, and the
  consumer creates a `Subscriber` for **every** event — so a scripted round
  trip must register *all* events (e.g. `left_tire` and `exhaust`), even the
  unused ones (register unscripted mocks to keep strictness).
- The mock `Subscription` receives are driven by a scripted `poll_samples`
  returning `PollOutcome::{Ready,Retry,Idle}`; the stream parks on `Idle`.
- `MockSample`/`MockSampleMut` only carry data; all behavior is in the
  `Mock*Policy` `#[automock]` traits. Cloned handles (publisher, provider
  info, service discovery, consumer builder) share one `Arc<Mutex<...>>`
  policy, so expectations are shared — matching how `interface!` clones them.
- `to_stream` returns an `impl Stream + Unpin`; `cancellable_receive` returns
  an opaque `impl Future` that is *not* `Unpin` at the call site (use
  `Box::pin` to poll it in a test).
- mockall `returning` closures take the method's arguments (e.g. `send` →
  `|value: T|`), not zero args. Guard patterns in `matches!` are unstable here —
  use plain `match` arms instead.
