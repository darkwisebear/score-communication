# AGENTS.md

Repository knowledge for future OpenHands sessions on this repo
(Eclipse `communication`, Bazel + rules_rust, Rust edition 2024).

## Plan A: functional fake COM runtime (issue #490)

A test-only, **purely hand-written** runtime implementing
`score_com_concept::Runtime` (no `mockall`), where samples published by an
offered producer are **actually delivered** to subscriptions over an in-memory
pub/sub registry — the complement of Plan B (mockall), where nothing happens
unless scripted.

- Crate: `score/mw/com/impl/rust/com-api/com-api-runtime-fake`
  (crate name `com_api_runtime_fake`, `testonly = True`, deps:
  `score_com_concept`, `futures` — **no mockall**).
- Shared state: `FakeRegistry` (`Arc`-cloned; every handle shares one).
  Instances live in a `Vec` keyed by `(interface_id, InstanceSpecifier)`
  **string form** because `InstanceSpecifier` is only `Clone + Debug` (no
  `Hash`/`Eq`/`Ord`). Each `(interface, instance, event)` owns one
  `EventQueue<T>` (type-erased as `Arc<dyn Any + Send + Sync>` in
  `ErasedQueue`, recovered with `downcast::<EventQueue<T>>()`).
- Data flow: `Publisher::send` appends `T` **by value** to the queue; a
  `Subscription` drains it (consume-on-read, retained until consumed, send
  order). `Sample` types **own** their `T` (no `Arc<T>` — see gotchas).
- Discovery is real: `find_service` converts `FindServiceSpecifier` to owned
  `Option<String>` eagerly (it is neither `Clone` nor `Send`); async discovery
  parks on an interface-level waiter list and is woken by
  `ProviderInfo::offer_service`.
- Re-export shim: `score/mw/com/rust/score_com_fake.rs` (target
  `//score/mw/com/rust:score_com_fake`, `testonly = True`,
  `crate_name = "score_com"` — mandatory, same reason as the mockall shim).
- Proof / example test:
  `score/mw/com/example/com-api-example/tests_using_fake_runtime.rs`
  (target `//score/mw/com/example/com-api-example:com-api-example-fake-runtime-test`,
  mirrors the tokio + mockall tests with the *same* generic helpers, no
  scripting, no config file; runs in CI).
- Crate unit tests (inline `#[cfg(test)]`, run by the `-tests` target): queue
  push/drain, lazy `queue_for` sharing, offer/unoffer, publisher→subscription
  round trip, replacement semantics, `to_stream`, `unsubscribe`.

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
# Plan A (functional fake)
bazel build //score/mw/com/impl/rust/com-api/com-api-runtime-fake:com-api-runtime-fake
bazel test  //score/mw/com/impl/rust/com-api/com-api-runtime-fake:com-api-runtime-fake-tests
bazel test  //score/mw/com/example/com-api-example:com-api-example-fake-runtime-test --test_output=all
# Plan B (mockall)
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
- **Plan A — own the sample, don't `Arc` it.** The `Runtime` GAT
  `type Subscriber<T: CommData + Debug> = …` forces the impl to hold for *any*
  `T: CommData + Debug`, and `Sample<T>: Send` requires `T: Sync`. `Arc<T>:
  Send` needs `T: Sync`, which `CommData` (`Reloc: Send + Unpin + 'static`)
  does *not* imply — so `FakeSample`/`FakeSampleMut` must **own** `T` by value
  (like mockall's `MockSample`), and the queue is a consume-on-read `VecDeque<T>`,
  not a retained `Arc<T>` broadcast.
- **Plan A — type-erase with `Arc<dyn Any + Send + Sync>`** (both `Send` *and*
  `Sync`), not `Arc<dyn Any + Send>`: the latter is not `Sync`, so it is not
  `Send`, breaking `ProviderInfo: Send`. Recover the typed queue via
  `downcast::<EventQueue<T>>()`; one event maps to exactly one `T`, so `None`
  is a panic (programming error).
- **Plan A — don't derive `Clone` on a struct holding `Arc<T>` where `T:
  CommData`** — the derive demands `T: Clone`. Write a manual `Clone` that only
  clones the `Arc`, and drop any `PhantomData<T>` that isn't needed (the queue
  already carries `T`'s type).
- **Plan A — scope locks so a waker can re-enter from another thread.**
  `FakeRegistry` locks the outer `instances` map only to locate/create an
  entry, drops it, then locks the per-entry state; `drain_or_wait` registers the
  waker *under the same queue lock* it checks for values (no lost wakeup).
  `Mutex` is **not** reentrant — a nested self-`lock()` on the same `Mutex`
  deadlocks.
- **Plan A — `FindServiceSpecifier` is neither `Clone` nor `Send`**, so convert
  it to owned `Option<String>` (`Some` = `Specific`, `None` = `Any`) *before*
  capturing it in any `Send` future; `Interface`'s `INTERFACE_ID` is a
  `&'static str`.
- **Plan A — a test that directly `.await`s async discovery with nothing else
  running deadlocks**: the parked discovery only wakes when a *producer* calls
  `offer_service`, so spawn the consumer discovery and the producer (which
  offers) concurrently. `Interface!`'s `offer()`/consumer `new` build a
  publisher/subscriber for *every* event — same as Plan B.
