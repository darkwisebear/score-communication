# Mock Runtime for the Rust COM API (mockall-based)

## Context / findings

- `score_com_concept::Runtime` (in `score/mw/com/rust/score_com_concept/concept.rs`, lines 89–140)
  is the root trait of the Rust COM API. Its associated types are **generic associated types
  (GATs)**, each additionally bounded to a concept trait (verified in source):
  `ServiceDiscovery<I: Interface + Send>: ServiceDiscovery<I, Self>`,
  `Subscriber<T: CommData + Debug>: Subscriber<T, Self>`,
  `ProducerBuilder<I: Interface>: ProducerBuilder<I, Self>`,
  `Publisher<T: CommData + Debug>: Publisher<T, Self>`, plus the plain (non-generic)
  `ProviderInfo: ProviderInfo + Send + Clone` and `ConsumerInfo: Send + Clone`.
- **mockall 0.14.0** is already vendored (`@score_communication_crate_index//:mockall`, via
  `score_crates 0.0.11` in `MODULE.bazel`) and used in this repo
  (`com-api-runtime-lola/BUILD`, `com-api-ffi-lola/BUILD` target `bridge_ffi_mock`, and the
  `com-api-runtime-mockall` blueprint). Verified against
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
  not wired to any `rust_test` target, and its `rust_library` is **not** `testonly`). It's
  referenced from `score/mw/com/rust/score_com_mock.rs` (a `testonly` shim, crate_name `score_com`)
  with the comment *"Mock Runtime is not yet ready — eclipse-score/communication#490"*. This
  confirms the upstream feature request this work closes.
- **Important: a partial Plan B blueprint already exists.** Commit `4f339524` ("Add blueprint for
  mocking of a trait with GATs") added
  `score/mw/com/impl/rust/com-api/com-api-runtime-mockall/` with `BUILD` (`testonly = True`,
  crate_name `com_api_runtime_mockall`), `lib.rs`, `provider_info.rs`, `publisher.rs`,
  `registry.rs`, `runtime.rs`, and `stubs.rs`. It already wires `@score_communication_crate_index//:mockall`
  and a `com-api-runtime-mockall-tests` `rust_test`. **However**, `lib.rs` was later **deleted by
  commit `577d7027`** (the current HEAD, "Updated add-mock-impl.md with the ground work done"), so
  the crate does not build **at HEAD** — `BUILD` still lists `lib.rs` among `srcs`, and `git show
  HEAD:.../com-api-runtime-mockall/lib.rs` fails because `HEAD` no longer contains the file (the
  worktree itself is clean). Plan B must build on / reconcile with this blueprint (restore `lib.rs`
  from `4f339524` or rewrite it) rather than assume an empty crate. Its existing files
  (`publisher.rs` GAT-wrapper workaround, `registry.rs`, `stubs.rs`) are a starting point, not a
  finished implementation.
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

> **Verified against the repo** (branch `add-mockall-mock-runtime`, 2026-09-25; re-verified
> 2026-09-28 against HEAD `577d7027`). Status per step:
> ✅ confirmed as written · ⚠️ confirmed with correction · ➕ new prerequisite discovered.

1. ⚠️ **Crate layout convention** — confirmed, with one correction. New crates go under
   `score/mw/com/impl/rust/com-api/<name>/{BUILD,*.rs}` as `rust_library` with visibility
   `//score/mw/com:__subpackages__`. The `testonly = True` attribute is **not** a shared convention
   of that directory: the legacy `com-api-runtime-mock` crate is *not* `testonly`, and neither is
   `bridge_ffi_mock` (`com-api-runtime-mock` is a plain `rust_library`; `bridge_ffi_mock` is a
   plain `rust_binary` — both without `testonly`). `testonly = True`
   *is* correct for the two new mock crates (they are test-only by purpose — the existing
   `com-api-runtime-mockall` blueprint already sets it) — keep it, but don't cite the other two
   targets as the precedent for it.
2. ✅ **mockall dependency** — confirmed. The label is
   `@score_communication_crate_index//:mockall` (repo name for `score_crates 0.0.11` in
   `MODULE.bazel`, providing `mockall 0.14.0`). Already used in `com-api-runtime-lola/BUILD`,
   `com-api-ffi-lola/BUILD` (`bridge_ffi_mock`), and the `com-api-runtime-mockall` blueprint `BUILD`.
   Plan A (pure hand-written fake) needs **no** mockall dep; only Plan B does.
3. ✅ **No `concept.rs` changes needed** — confirmed. `score_com_concept`'s `rust_library`
   (`score/mw/com/rust/score_com_concept/BUILD`) depends only on `paste`, `futures`, `thiserror`,
   baselibs — no mockall/test-only deps — so keeping mocking in downstream crates leaves it clean.
4. ✅ **Re-export shims** — pattern confirmed, current state documented. The existing
   `score/mw/com/rust/score_com_mock.rs` target (`name = "score_com_mock"`, `testonly = True`,
   `crate_name = "score_com"`, visibility `//visibility:public`) does `pub use score_com::*;` plus
   re-exports of the (legacy) `MockRuntimeImpl`/`MockRuntimeBuilderImpl` — i.e. it is a *fork* of the
   `score_com` crate, not just an add-on. The two new shims should follow this shape:
   `score/mw/com/rust/score_com_fake.rs` (Plan A) and `score/mw/com/rust/score_com_mockall.rs`
   (Plan B), each with its own `testonly` `rust_library` target in `score/mw/com/rust/BUILD`. Note
   `score_com_mock` currently points at the broken legacy stub — see open follow-ups.
5. ⚠️ **Example/integration tests** — reference test confirmed, with details.
   `stream_and_send_using_multi_thread` lives at line 283 of
   `score/mw/com/example/com-api-example/tests_using_tokio_runtime.rs` (320 lines, single
   `#[cfg(test)] mod test`). Facts relevant to reusing it:
   - The test is `#[tokio::test(flavor = "multi_thread")]`; the runtime is a
     `OnceLock<score_com::LolaRuntimeImpl>` singleton (`get_test_runtime()`), and the Lola
     target is tagged `manual` with `data = ["etc/mw_com_config.json"]` (config-dependent).
   - It reuses `com_api_gen::{Tire, VehicleInterface, VehicleOfferedProducer}` and
     `score_com::{...}` — both confirmed available (see item 6).
   - Helper fns to adapt verbatim: `create_producer<R: Runtime>` (builds + offers a
     `VehicleOfferedProducer<R>`), `create_consumer_async<R: Runtime>` (async discovery →
     `VehicleConsumer<R>`), `async_data_sender_fn` (5 `Tire` samples, 1 s interval, via
     `allocate()`/`write()`/`send()`), `stream_processor_fn` (`subscribed.to_stream()` +
     `futures::stream::StreamExt::next()` with per-sample timeout).
   - **Correction to the "not tagged `manual`" plan**: the new fake/mockall test targets should be
     `rust_test` with `features = ["link_std_cpp_lib"]` (required — `com_api_gen` links the C++
     `vehicle_gen_cpp` object; see the existing tokio target), and must **not** require the
     `etc/mw_com_config.json` data file (which is why the Lola target is `manual`). Omitting the
     data dep and the config path is what lets them run in normal CI; no `manual` tag.
   New files (names confirmed available in the tree):
   - `score/mw/com/example/com-api-example/tests_using_fake_runtime.rs` (Plan A)
   - `score/mw/com/example/com-api-example/tests_using_mockall_runtime.rs` (Plan B)
6. ➕ **Example generator exports** (needed by step 5) — confirmed.
   `com-api-gen/com_api_gen.rs` defines `Tire` (`pressure: f32`, derives `Debug, Reloc, CommData,
   ScoreDebug`, `#[repr(C)]`, `#[comm_data(id = "Tire")]`), an empty `Exhaust` struct (auto
   `CommData` id), and `interface!(interface Vehicle { Id = "VehicleInterface", left_tire:
   Event<Tire>, exhaust: Event<Exhaust> })`. The crate (`com_api_gen`, `crate_name = "com_api_gen"`,
   `features = ["link_std_cpp_lib"]`) therefore exports `VehicleInterface`, `VehicleConsumer<R>`,
   `VehicleProducer<R>`, `VehicleOfferedProducer<R>` with the events as struct fields
   (e.g. `consumer.left_tire`, a per-event `Subscriber`-typed field — exactly how the tokio test
   uses `consumer.left_tire.subscribe(...)`).
7. ➕ **Plan B repair of the committed `lib.rs` deletion** (new, discovered during verification) —
   the deletion is **committed in `577d7027` (current HEAD)**, not a worktree artifact (the
   worktree is clean; `git show HEAD:.../com-api-runtime-mockall/lib.rs` fails because the file no
   longer exists in `HEAD`, so restore-from-`HEAD` is impossible). Before any Plan B work, restore
   `lib.rs` from the only git-recoverable source, commit `4f339524`: `git show
   4f339524:score/mw/com/impl/rust/com-api/com-api-runtime-mockall/lib.rs` (which declares
   `provider_info/publisher/registry/runtime/stubs` and re-exports `ProviderInfoHandle`, the
   `MockPublisherImpl`/`MockPublisherPolicy`/`MockSampleMaybeUninit`/`MockSampleMut`/`PublisherPolicy`
   set, `MockConsumerInfo`/`MockRegistry`, and `MockRuntimeBuilderImpl`/`MockRuntimeImpl`) or rewrite
   it to match the final file set. `BUILD` (`srcs` list, `crate_name = "com_api_runtime_mockall"`,
   `com-api-runtime-mockall-tests` target) is already in place and needs to stay consistent with
   whatever the final `srcs` are.

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

Crate `score/mw/com/impl/rust/com-api/com-api-runtime-mockall` — **partially exists already**
(blueprint from commit `4f339524`: `BUILD` with `testonly = True` + `crate_name =
com_api_runtime_mockall` + `com-api-runtime-mockall-tests` `rust_test`; `lib.rs` [committed deletion
in `577d7027` — see groundwork item 7], `provider_info.rs`, `publisher.rs`, `registry.rs`,
`runtime.rs`, `stubs.rs`). Verified contents:
- `provider_info.rs`: `ProviderInfoHandle` — a **plain** `#[derive(Clone, Debug)]` struct carrying
  `instance: InstanceSpecifier` + the shared `MockRegistry`; `offer_service`/`stop_offer_service`
  currently succeed unconditionally (scriptable offer behavior is explicitly marked out of scope
  for that slice). Not `#[automock]`-backed, despite the per-trait table — Plan B must decide
  whether to upgrade it.
- `publisher.rs`: the GAT workaround — `PublisherPolicy<T>` (plain, lifetime-free trait),
  `MockPublisherPolicy` (`#[automock]`), `MockSampleMaybeUninit<'a, T>` / `MockSampleMut<'a, T>`,
  and `MockPublisherImpl<T>` holding `Arc<Mutex<MockPublisherPolicy<T>>>`; `new()` looks the policy
  up in the registry and **panics** if none was registered.
- `registry.rs`: `MockRegistry` = `Arc<Mutex<HashMap<SlotKey, Box<dyn Any + Send>>>>`
  (type-erased per-slot storage) plus `MockConsumerInfo` (plain struct).
- `runtime.rs`: `MockRuntimeImpl` (clones share the registry) with GAT slots bound to the
  `Stub*` types for `ServiceDiscovery`/`Subscriber`/`ProducerBuilder`, `MockPublisherImpl<T>` for
  `Publisher`, `ProviderInfoHandle`, and `MockConsumerInfo`; `MockRuntimeBuilderImpl` is an empty
  struct.
- `stubs.rs`: minimal non-mockable placeholders `StubSample`, `StubSubscriber`,
  `StubSubscription`, `StubServiceDiscovery`, `StubConsumerBuilder`, `StubProducerBuilder`
  covering the remaining GAT slots.

Plan B therefore starts from this partial crate (after restoring its `lib.rs` per groundwork item
7), completes/replaces the stubs per the per-trait table above (and aligns `MockProviderInfo` handling with the table or amends the table),
and keeps the existing BUILD targets intact.

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

- **First**: restore `com-api-runtime-mockall/lib.rs` so the existing blueprint builds again
  (groundwork item 7). `git checkout --`/restore-from-`HEAD` is **not** possible — the deletion is
  committed (`577d7027`) and `HEAD` no longer contains the file — so use
  `git show 4f339524:score/mw/com/impl/rust/com-api/com-api-runtime-mockall/lib.rs >
  score/mw/com/impl/rust/com-api/com-api-runtime-mockall/lib.rs` (or rewrite it to match the final
  file set). Then `bazel build //score/mw/com/impl/rust/com-api/com-api-runtime-mockall/...` must
  pass *before* Plan B changes land.
- `bazel build //score/mw/com/impl/rust/com-api/com-api-runtime-fake/... //score/mw/com/impl/rust/com-api/com-api-runtime-mockall/...`
- `bazel test //score/mw/com/impl/rust/com-api/com-api-runtime-mockall:com-api-runtime-mockall-tests`
  (the blueprint's own crate-level unit test, already wired in its BUILD) plus the two new
  example tests:
  `bazel test //score/mw/com/example/com-api-example:com-api-example-fake-runtime-test //score/mw/com/example/com-api-example:com-api-example-mockall-runtime-test`
  (final target names TBD during implementation).
- Existing `score_com`/`score_com_concept`/`com-api-example` targets must remain green
  (`bazel test //score/mw/com/...` for anything touched). Note the Lola-backed
  `com-api-example-tokio-integration-test` is tagged `manual` — it is not part of the default CI
  set, so "remain green" means the non-manual subset plus a manual spot-check.
- No changes expected to `concept.rs`, so its existing doctest/unit-test targets
  (`score_com_concept-test`, `score_com_concept-macros-tests`,
  `score_com_concept-macros-unit-tests` — all `manual`/scoped) are unaffected unless incidentally
  touched.

## Open follow-ups (not blocking, revisit after both crates land)

- Decide the fate of the legacy `com-api-runtime-mock` stub crate (delete vs. keep vs. merge into
  Plan A).
- Whether `score_com_mock` (existing test-only re-export) should be repointed at one of the two
  new crates, replaced by two new re-export targets, or left alone.
- The Plan B blueprint has already drifted from this doc's "classical mockall" description
  (registry-backed `MockPublisherImpl`, unconditional `offer_service`, `Stub*` placeholders).
  Decide, per trait, whether to follow the blueprint's evolving approach or the per-trait table —
  and reconcile the doc's table with the chosen approach.
- Restore the committed-deleted `com-api-runtime-mockall/lib.rs` (from `4f339524` vs. rewrite) as
  part of the first Plan B commit, so the branch builds — `HEAD` no longer contains the file, so
  restore-from-`HEAD` is not an option.
- Commit messages will include `Closes #490` per user instruction (eclipse-score/communication).
