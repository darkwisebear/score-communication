# Mock Runtime for the Rust COM API (mockall-based)

## Context / findings

- `score_com_concept::Runtime` (in `concept.rs`) is the root trait of the Rust COM API. Its
  associated types are **generic associated types (GATs)**:
  `ServiceDiscovery<I: Interface + Send>`, `Subscriber<T: CommData + Debug>`,
  `ProducerBuilder<I: Interface>`, `Publisher<T: CommData + Debug>`, plus the plain
  (non-generic) `ProviderInfo` and `ConsumerInfo`.
- **mockall 0.14.0** is already vendored (`@score_communication_crate_index//:mockall`) and used
  in this repo (`com-api-runtime-lola`, `com-api-ffi-lola/bridge_ffi_mock.rs`). Verified against
  mockall's docs/source and this repo's own precedent:
  - `#[automock]` / `mock!` **cannot** handle GAT associated types (`type Foo<'a>`, `type
    Foo<I>`) — confirmed limitation, no workaround exists in 0.14.
  - `#[automock]` **can** handle: generic traits (params on the trait itself), plain (non-GAT)
    associated types (via metaitems), `impl Future`/`impl Stream` return values, methods with a
    single lifetime parameter (with restrictions: no `with()`, lifetime can't leak into the
    return type, can't mix generic lifetimes with generic type params on the same method).
  - `mock!` additionally handles supertrait bounds / multiple trait impls (`Sample<T>: Deref +
    Send + PartialOrd + Ord + Debug` etc.), which plain `#[automock]` rejects.
  - This repo already established the exact hybrid pattern we need in
    `com-api-ffi-lola/bridge_ffi_mock.rs`: a `mock!`-generated inner mock plus a hand-written
    wrapper (there: for `Clone`/shared-state; for us: for the GAT slots).
- There is an **existing, incomplete stub crate**
  `score/mw/com/impl/rust/com-api/com-api-runtime-mock` (hand-rolled, full of `todo!()`, its
  `#[cfg(test)] mod test` doesn't even match current `SampleContainer`/`Subscriber` signatures,
  not wired to any `rust_test` target). It's referenced from `score/mw/com/rust/score_com_mock.rs`
  with the comment *"Mock Runtime is not yet ready — eclipse-score/communication#490"*. This
  confirms the upstream feature request this work closes.
- Per user decision, we build **two independent crates**, each a full answer to "make `Runtime`
  mockable with mockall", but with different behavioral philosophy:
  - **Plan A — Functional fake backend**: a working in-memory pub/sub simulation (samples
    published by a producer are actually delivered to matching subscribers). Useful as a drop-in
    backend for users who want to exercise their own business logic without any real IPC.
  - **Plan B — Classical mockall backend**: every call is a real mockall expectation; nothing
    happens unless the test scripts it. Useful for fine-grained unit tests of code that consumes
    `Runtime` (verify exact call sequences/arguments, inject specific errors, etc.).
  - Scope of "associated types to mock": `Runtime`'s own 6 associated types
    (`ServiceDiscovery`, `Subscriber`, `ProducerBuilder`, `Publisher`, `ProviderInfo`,
    `ConsumerInfo`) plus whatever transitive traits (`Subscription`, `ConsumerBuilder`,
    `ConsumerDescriptor`, `Sample`, `SampleMut`, `SampleMaybeUninit`, `Builder`) are structurally
    required to make them usable. `Producer`/`Consumer`/`OfferedProducer`/`Interface` are already
    implemented generically by the `interface!` macro (`com_api_gen.rs`) — not reimplemented here.
  - The existing broken `com-api-runtime-mock` stub is left untouched for now; its fate (deleted,
    kept as legacy, or superseded) is called out as a follow-up decision once the two new crates
    exist and prove themselves.

## Per-trait mocking technique (applies to both plans where the trait is reused)

| Trait | GAT? | Supertrait bounds? | Technique |
|---|---|---|---|
| `Runtime` | yes (4 assoc types) | no | hand-written struct; GAT slots point to the mocked types below |
| `ProviderInfo` | no | no | `#[automock]` directly, wrapped in `Arc<Mutex<..>>` newtype for `Clone`+`Send` (mirrors `SharedMockBridge`) |
| `ConsumerInfo` | n/a (no trait, just data bound `Send + Clone`) | – | plain `#[derive(Clone, Debug)]` struct, no mockall needed |
| `ServiceDiscovery<I,R>` | no | no | `#[automock]` (generic over `I`), metaitems for `ConsumerBuilder`/`ServiceEnumerator` |
| `ConsumerBuilder<I,R>` / `ConsumerDescriptor<R>` | no | trait-combo (`ConsumerDescriptor + Builder<...>`) | `mock!` (combined impl block) |
| `ProducerBuilder<I,R>` (= `Builder<I::Producer<R>>`) | no | trait-combo | `mock!` |
| `Subscriber<T,R>` | no | no | `#[automock]` (generic over `T`) |
| `Subscription<T,R>` | yes (`Sample<'a>`) | no | hand-written wrapper; behavior delegated to an internal `#[automock]`-ed "policy" trait without the lifetime-tied method |
| `Publisher<T,R>` | yes (`SampleMaybeUninit<'a>`) | no | hand-written wrapper; same policy-delegation trick |
| `Sample<T>` / `SampleMut<T>` / `SampleMaybeUninit<T>` | no (but return/borrow `'a` data) | yes (`Deref`/`DerefMut`/`AsMut`/`Debug`/…) | thin hand-written structs (little behavior worth scripting); not mockall-based in either plan |
| `Builder<Output>` | no | no | `#[automock]` where used standalone (e.g. inside `mock!` combo blocks above) |

## Common groundwork (both plans)

1. Confirm crate layout convention: new crates under
   `score/mw/com/impl/rust/com-api/<name>/{BUILD,*.rs}`, `rust_library`, `testonly = True`,
   visibility `//score/mw/com:__subpackages__` (matches `com-api-runtime-mock` /
   `bridge_ffi_mock`).
2. Add `@score_communication_crate_index//:mockall` as a dep where mockall is actually used.
3. No changes to `score_com_concept::concept.rs` are required — mocking happens entirely in the
   new downstream crates, so the core trait-definition crate stays free of test-only
   dependencies/attributes.
4. Each plan gets its own re-export shim analogous to `score_com_mock.rs`
   (e.g. `score/mw/com/rust/score_com_fake.rs` re-exporting Plan A's `FakeRuntimeImpl`, and
   `score/mw/com/rust/score_com_mockall.rs` re-exporting Plan B's `MockRuntimeImpl`), each with
   its own `testonly` `rust_library` target in `score/mw/com/rust/BUILD`, so downstream users can
   pick exactly one without pulling in the other's dependencies.
5. Both plans need an example/integration test that "resembles"
   `stream_and_send_using_multi_thread` from `tests_using_tokio_runtime.rs`, reusing
   `com_api_gen::{Tire, VehicleInterface, VehicleOfferedProducer}` and the same overall
   producer/consumer-over-tokio-tasks shape, adapted to each backend's semantics. New files:
   - `score/mw/com/example/com-api-example/tests_using_fake_runtime.rs` (Plan A)
   - `score/mw/com/example/com-api-example/tests_using_mockall_runtime.rs` (Plan B)
   Both wired as `rust_test` targets in `com-api-example/BUILD`, **not** tagged `manual` (no real
   backend/config file needed), so they run in normal CI — unlike the existing Lola-backed
   `com-api-example-tokio-integration-test`.

## Plan A — Functional fake backend (`com-api-runtime-fake`)

New crate `score/mw/com/impl/rust/com-api/com-api-runtime-fake`.

- `FakeRuntimeImpl`: hand-written, holds an `Arc<Mutex<Registry>>` shared by everything it hands
  out. `Registry` maps `(interface INTERFACE_ID, InstanceSpecifier)` → a type-erased, per-event
  broadcast queue (`Arc<Mutex<VecDeque<T>>>` behind `Any`/small enum, or a
  `tokio::sync::broadcast`-style channel) so a `Publisher<T>` created by an offered producer and a
  `Subscriber<T>`/`Subscription<T>` created by a discovered consumer for the *same* instance +
  event actually exchange data.
- `find_service` / `producer_builder`: real implementations that consult/populate the registry
  (service discovery only returns instances that have actually been offered).
- `FakeProviderInfo::offer_service`/`stop_offer_service`: register/deregister the instance in the
  registry; still exposed as `#[automock]`-backed (wrapped in `Arc<Mutex<_>>`) so tests *can*
  inject a failure or assert call counts, even though the default behavior "just works".
- `FakeSubscription::try_receive` / `cancellable_receive` / `to_stream`: real implementations
  pulling from the shared queue (fixing the `todo!()`/`stream::empty()` gaps in the current stub).
- `FakePublisher::allocate` / `send`: real implementation pushing into the shared queue.
- Result: `async_data_sender_fn` / `stream_processor_fn` (reused almost verbatim from the tokio
  test) actually observe the 5 published `Tire` samples through the fake backend, with **no**
  manual expectation scripting required — closest in spirit to the real Lola runtime test.
- Unit tests for the crate itself (registry lookup, capacity behavior, error paths) plus the
  `tests_using_fake_runtime.rs` integration test reproducing
  `stream_and_send_using_multi_thread`.

## Plan B — Classical mockall backend (`com-api-runtime-mockall`)

New crate `score/mw/com/impl/rust/com-api/com-api-runtime-mockall`.

- `MockRuntimeImpl`: hand-written, GAT slots bound to:
  `MockServiceDiscoveryImpl<I>`, `MockSubscriberImpl<T>`, `MockProducerBuilderImpl<I>`,
  `MockPublisherImpl<T>` — all real mockall mock structs (or thin GAT wrappers around one, per the
  table above). `find_service`/`producer_builder` simply construct a **fresh, unconfigured**
  instance of the relevant mock and return it by value — the calling test configures
  `.expect_...()` on the object it gets back before exercising it further (standard mockall usage;
  no attempt to make `Runtime`'s own methods themselves expectation-driven, since that's exactly
  the GAT wall).
- `MockProviderInfo`: `#[automock]`, wrapped in `Arc<Mutex<_>>` (`ProviderInfoHandle`) for
  `Clone + Send`, mirroring `SharedMockBridge`.
- `MockSubscription<T>`/`MockPublisher<T>` GAT wrappers delegate their actual decision logic
  (how many samples to yield, success/failure, captured sent values) to an internal
  `#[automock]`-ed "policy" trait that has no lifetime-tied associated types (e.g.
  `SubscriptionPolicy<T>` with plain-value methods only); the outer hand-written wrapper is the
  only place touching `Self::Sample<'a>` / `Self::SampleMaybeUninit<'a>`.
- The reimplemented test `tests_using_mockall_runtime.rs` mirrors
  `stream_and_send_using_multi_thread` but every interaction is scripted explicitly:
  `expect_offer_service()`, `expect_get_available_instances_async()` (returning a canned
  `MockConsumerBuilder`), `expect_subscribe()`/the policy's `expect_...()` (returning a canned
  stream of the same 5 `Tire` values the producer is expected to send), `expect_allocate()` /
  `expect_send()` with `mockall::predicate::eq(...)` argument matchers, optionally sequenced with
  `mockall::Sequence` (precedent: `producer.rs`/`consumer.rs` in `com-api-runtime-lola` already do
  this). This test explicitly demonstrates/validates that `Runtime` + its 6 associated types are
  "mockable with the tools mockall offers", per the original request.

## Validation

- `bazel build //score/mw/com/impl/rust/com-api/com-api-runtime-fake/... //score/mw/com/impl/rust/com-api/com-api-runtime-mockall/...`
- `bazel test //score/mw/com/example/com-api-example:com-api-example-fake-runtime-test //score/mw/com/example/com-api-example:com-api-example-mockall-runtime-test` (final target names TBD during implementation)
- Existing `score_com`/`score_com_concept`/`com-api-example` targets must remain green
  (`bazel test //score/mw/com/...` for anything touched).
- No changes expected to `concept.rs`, so its existing doctest/unit-test targets are unaffected
  unless incidentally touched.

## Open follow-ups (not blocking, revisit after both crates land)

- Decide the fate of the legacy `com-api-runtime-mock` stub crate (delete vs. keep vs. merge into
  Plan A).
- Whether `score_com_mock` (existing test-only re-export) should be repointed at one of the two
  new crates, replaced by two new re-export targets, or left alone.
- Commit messages will include `Closes #490` per user instruction (eclipse-score/communication).
