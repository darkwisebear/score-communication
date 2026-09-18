# Plan B — Implementation Guide: `com-api-runtime-mockall`

Step-by-step instructions for implementing **Plan B** from `add-mock-impl.md`: a *classical
mockall* backend for `score_com_concept::Runtime`, where nothing happens unless the test scripts
it.

Everything below is grounded in the current sources:

| Item | Path |
|---|---|
| Trait definitions | `score/mw/com/rust/score_com_concept/concept.rs` |
| `interface!` codegen | `score/mw/com/rust/score_com_concept/interface_macros.rs` |
| Reference impl (real) | `score/mw/com/impl/rust/com-api/com-api-runtime-lola/{runtime,producer,consumer}.rs` |
| Hybrid mockall precedent | `score/mw/com/impl/rust/com-api/com-api-ffi-lola/bridge_ffi_mock.rs` |
| Re-export shim precedent | `score/mw/com/rust/score_com_mock.rs` + `score/mw/com/rust/BUILD` |
| Test to mirror | `score/mw/com/example/com-api-example/tests_using_tokio_runtime.rs` (`stream_and_send_using_multi_thread`) |

---

## 0. The two problems you must solve first

Read this section before writing any code — it determines the whole crate layout.

### Problem 1: GATs block `#[automock]`/`mock!`

Three traits carry generic associated types and therefore **cannot** be mocked by mockall 0.14:

```rust
trait Runtime {                       // 4 GATs
    type ServiceDiscovery<I: Interface + Send>: ServiceDiscovery<I, Self>;
    type Subscriber<T: CommData + Debug>: Subscriber<T, Self>;
    type ProducerBuilder<I: Interface>: ProducerBuilder<I, Self>;
    type Publisher<T: CommData + Debug>: Publisher<T, Self>;
    type ProviderInfo: ProviderInfo + Send + Clone;   // plain
    type ConsumerInfo: Send + Clone;                  // plain
}
trait Publisher<T, R> { type SampleMaybeUninit<'a>: SampleMaybeUninit<T> + 'a where Self: 'a; … }
trait Subscription<T, R> { type Sample<'a>: Sample<T> where Self: 'a; … }
```

**Technique (same for all three):** hand-write the outer type, move every scriptable decision into
an internal, lifetime-free *policy trait* that **is** `#[automock]`-able. The hand-written wrapper
is the only place that ever names `Self::Sample<'a>` / `Self::SampleMaybeUninit<'a>`.

**Blueprint:** the Publisher workaround is implemented in
[`score/mw/com/impl/rust/com-api/com-api-runtime-mockall/publisher.rs`](score/mw/com/impl/rust/com-api/com-api-runtime-mockall/publisher.rs).
Copy this policy + wrapper split for `Subscription` (and any other GAT trait). Tests may use
`MockPublisherImpl::from_policy()`; production construction goes through `Publisher::new` + the
registry.

### Problem 2: you never get to touch the objects the `interface!` macro constructs

`interface_macros.rs` generates code that constructs endpoints **internally**:

```rust
// VehicleConsumer::new  (interface_consumer!)
$event_name: R::Subscriber::new(stringify!($event_name), instance_info.clone())…
// VehicleProducer::offer  (interface_producer!)
$event_name: R::Publisher::new(stringify!($event_name), self.instance_info.clone())…
self.instance_info.offer_service()?;
```

So `Subscriber::new` / `Publisher::new` are **static trait methods called by generated code**. A
test can never hand a pre-configured `MockPublisher` into `VehicleOfferedProducer`.

Two ways out:

* **(a) mockall static-method contexts** (`MockFoo::<T>::new_context()`). Works, but expectations
  are **global**, must be serialized across tests, and are hostile to
  `#[tokio::test(flavor = "multi_thread")]`. **Do not use.**
* **(b) Registry injection — use this.** `ConsumerInfo` and `ProviderInfo` are `Send + Clone` and
  are threaded by the macro into every constructor. Put a shared, type-erased **`MockRegistry`**
  inside them. The test registers policy mocks up front keyed by
  `(InstanceSpecifier, event identifier)`; `Subscriber::new` / `Publisher::new` pop them out.

This keeps the "classical mockall" philosophy: the *behavior* still comes exclusively from
`expect_*()` calls written by the test.

---

## 1. Crate skeleton

Create `score/mw/com/impl/rust/com-api/com-api-runtime-mockall/` with:

```
BUILD
lib.rs              crate docs + `mod`/`pub use` (mirror com-api-runtime-lola/lib.rs)
registry.rs         MockRegistry, slot keys, MockConsumerInfo
runtime.rs          MockRuntimeImpl, MockRuntimeBuilderImpl
provider_info.rs    MockProviderInfo (automock) + ProviderInfoHandle
service_discovery.rs MockServiceDiscoveryImpl, MockConsumerBuilderImpl
producer_builder.rs MockProducerBuilderImpl
publisher.rs        PublisherPolicy + MockPublisherImpl + sample types
subscriber.rs       SubscriberPolicy/SubscriptionPolicy + MockSubscriberImpl/MockSubscriptionImpl + MockSample
prelude.rs          (optional) ergonomic helpers re-exported for tests
```

`BUILD`:

```python
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "com-api-runtime-mockall",
    testonly = True,
    srcs = [
        "lib.rs", "registry.rs", "runtime.rs", "provider_info.rs",
        "service_discovery.rs", "producer_builder.rs", "publisher.rs", "subscriber.rs",
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
    deps = ["@score_communication_crate_index//:mockall"],
)
```

Notes:
* `mockall` 0.14.0 already resolves via `@score_communication_crate_index` (bazel_dep
  `score_crates`, see `MODULE.bazel`) — **no manifest change needed**.
* `testonly = True` and visibility `//score/mw/com:__subpackages__` mirror
  `com-api-runtime-mock` / `bridge_ffi_mock`.
* Prepend the Eclipse Apache-2.0 copyright header to every new `.rs` and `BUILD` file (copy the
  header block from any existing file in the tree).

**Milestone check:** `bazel build //score/mw/com/impl/rust/com-api/com-api-runtime-mockall/...`
with only `lib.rs` present. Do this *now*, before writing logic.

---

## 2. `registry.rs` — expectation injection

```rust
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use score_com_concept::InstanceSpecifier;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct SlotKey {
    instance: String,        // InstanceSpecifier::as_ref().to_owned()
    event: String,           // "left_tire", "exhaust", … ("" for instance-level slots)
    kind: SlotKind,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum SlotKind { Subscriber, Subscription, Publisher, ServiceDiscovery, ProducerBuilder }

/// Shared, type-erased store of pre-configured mocks. Cloning shares the same store.
#[derive(Clone, Default)]
pub struct MockRegistry(Arc<Mutex<HashMap<SlotKey, Box<dyn Any + Send>>>>);

impl MockRegistry {
    pub fn new() -> Self { Self::default() }

    pub(crate) fn put<V: Any + Send>(&self, key: SlotKey, value: V) { … }
    pub(crate) fn take<V: Any + Send>(&self, key: &SlotKey) -> Option<V> { … } // downcast
}
```

Rules:
* `put` **replaces** and returns nothing; `take` **removes** the slot (each mock is consumed once).
* Provide typed helpers on `MockRegistry`, one pair per slot kind, e.g.
  `expect_publisher::<T>(&self, instance: &InstanceSpecifier, event: &str, policy: MockPublisherPolicy<T>)`
  and a private `take_publisher::<T>(…)`.
* On `take` miss, **do not** silently succeed. Return `None` and let the caller build a fresh,
  unconfigured mock (mockall then panics on the first unexpected call with a clear message), or
  panic with a message naming the missing `(instance, event, kind)`. Pick one policy and apply it
  consistently; the panic-with-context variant debugs much faster — recommended.
* `Debug` impl: print the key set only (mock objects are not `Debug`).

Also in this file:

```rust
#[derive(Clone)]
pub struct MockConsumerInfo {
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: MockRegistry,
}
```
This satisfies `Runtime::ConsumerInfo: Send + Clone` (no `ConsumerInfo` trait exists).

---

## 3. `provider_info.rs` — `ProviderInfo` (plain trait ⇒ `#[automock]`)

```rust
use mockall::automock;
use score_com_concept::{ProviderInfo, Result};

#[automock]
pub trait ProviderInfoPolicy: Send + 'static {
    fn offer_service(&self) -> Result<()>;
    fn stop_offer_service(&self) -> Result<()>;
}

/// `Runtime::ProviderInfo` — must be `Send + Clone`, and all clones must share expectations.
/// Mirrors `SharedMockBridge` in bridge_ffi_mock.rs.
#[derive(Clone)]
pub struct ProviderInfoHandle {
    policy: Arc<Mutex<MockProviderInfoPolicy>>,
    pub(crate) instance: InstanceSpecifier,
    pub(crate) registry: MockRegistry,
}

impl ProviderInfoHandle {
    pub fn new(instance: InstanceSpecifier, registry: MockRegistry,
               policy: MockProviderInfoPolicy) -> Self { … }
    /// Post-hoc assertions / late expectations, like `SharedMockBridge::with_mock`.
    pub fn with_mock<F, R>(&self, f: F) -> R where F: FnOnce(&mut MockProviderInfoPolicy) -> R { … }
}

impl ProviderInfo for ProviderInfoHandle {
    fn offer_service(&self) -> Result<()> { self.policy.lock().expect("poisoned").offer_service() }
    fn stop_offer_service(&self) -> Result<()> { … }
}
```

Why `Arc<Mutex<_>>`: `interface_producer!` clones `instance_info` once per event plus once for the
`OfferedProducer`; without sharing, `expect_offer_service()` set by the test would land on a
different object. The `Mutex` also makes the handle `Sync`, required because the test moves the
producer into a `tokio::spawn`ed task.

---

## 4. `publisher.rs` — GAT wall #1

**Implemented blueprint:** [`score/mw/com/impl/rust/com-api/com-api-runtime-mockall/publisher.rs`](score/mw/com/impl/rust/com-api/com-api-runtime-mockall/publisher.rs).
Subsequent GAT slices (`Subscription`) should copy that file’s policy + wrapper split. Samples
clone the `Arc<Mutex<MockPublisherPolicy<T>>>` (GAT lifetime is a `PhantomData<&'a ()>`) so the
sample does not borrow the publisher; `from_policy()` is test-only.

### 4.1 Policy trait (lifetime-free ⇒ automockable)

```rust
#[automock]
pub trait PublisherPolicy<T: CommData + Debug + 'static>: Send + 'static {
    /// Called by `Publisher::allocate`. Returning `Err` injects an allocation failure.
    fn allocate(&self) -> Result<()>;
    /// Called by `SampleMut::send` with the fully written value — the place for
    /// `mockall::predicate::eq(Tire { pressure: 3.0 })` style argument matching.
    fn send(&self, value: T) -> Result<()>;
}
```

`T: CommData` already implies `Send + Unpin + 'static` via `Reloc`, so mockall's `'static` and
`Send` requirements on generic parameters are satisfied.

### 4.2 Sample types (hand-written, no mockall — they carry no scriptable behavior)

```rust
pub struct MockSampleMaybeUninit<'a, T: CommData + Debug> {
    data: MaybeUninit<T>,
    policy: &'a Arc<Mutex<MockPublisherPolicy<T>>>,
}
impl<'a, T> AsMut<MaybeUninit<T>> for MockSampleMaybeUninit<'a, T> { … }
impl<'a, T> Debug for MockSampleMaybeUninit<'a, T> { … }   // hand-written: MaybeUninit isn't Debug
impl<'a, T: CommData + Debug> SampleMaybeUninit<T> for MockSampleMaybeUninit<'a, T> {
    type SampleMut = MockSampleMut<'a, T>;
    fn write(self, value: T) -> Self::SampleMut { … }
    unsafe fn assume_init(self) -> Self::SampleMut { … }    // self.data.assume_init()
}

pub struct MockSampleMut<'a, T: CommData + Debug> {
    data: T,
    policy: &'a Arc<Mutex<MockPublisherPolicy<T>>>,
}
impl<'a, T> Deref/DerefMut/Debug for MockSampleMut<'a, T> { … }
impl<'a, T: CommData + Debug> SampleMut<T> for MockSampleMut<'a, T> {
    fn send(self) -> Result<()> { self.policy.lock().expect("poisoned").send(self.data) }
}
```

Keep `#![allow(clippy::needless_lifetimes)]` at crate level as the other runtime crates do.

### 4.3 Wrapper implementing `Publisher`

```rust
pub struct MockPublisherImpl<T: CommData + Debug> {
    policy: Arc<Mutex<MockPublisherPolicy<T>>>,
}

impl<T: CommData + Debug> Publisher<T, MockRuntimeImpl> for MockPublisherImpl<T> {
    type SampleMaybeUninit<'a> = MockSampleMaybeUninit<'a, T> where Self: 'a;

    fn allocate(&self) -> Result<Self::SampleMaybeUninit<'_>> {
        self.policy.lock().expect("poisoned").allocate()?;   // expectation #1
        Ok(MockSampleMaybeUninit { data: MaybeUninit::uninit(), policy: &self.policy })
    }

    fn new(identifier: &str, instance_info: ProviderInfoHandle) -> Result<Self> {
        let policy = instance_info.registry
            .take_publisher::<T>(&instance_info.instance, identifier)
            .unwrap_or_else(|| panic!(
                "no MockPublisherPolicy registered for {}::{identifier}",
                instance_info.instance.as_ref()));
        Ok(Self { policy: Arc::new(Mutex::new(policy)) })
    }
}
```

Note `Publisher::send` has a **default body** (`allocate` → `write` → `send`), so both the
`send(value)` shortcut and the explicit `allocate/write/send` sequence in
`async_data_sender_fn` funnel through `expect_allocate()` + `expect_send()`. Tests must script
both.

---

## 5. `subscriber.rs` — GAT wall #2 + the async/stream semantics

### 5.1 Sample

```rust
pub struct MockSample<T: CommData + Debug> { id: usize, data: Box<T> }
```
`Sample<T>: Deref<Target = T> + Send + PartialOrd + Ord + Debug`. Order by a process-wide
`AtomicUsize` counter (reception order) — same trick as the old stub. The type needs no lifetime;
the GAT slot is `type Sample<'a> = MockSample<T>;` (`LolaSample` does exactly this).

### 5.2 Policies

```rust
/// Outcome of one scripted poll. Keeps async paths deterministic and spin-free.
#[derive(Debug)]
pub enum PollOutcome<T> {
    /// Deliver these samples now (may be empty ⇒ treated as `Retry`).
    Ready(Vec<T>),
    /// Nothing yet, but more is expected: wake the task and poll the policy again.
    Retry,
    /// Nothing, and nothing more is coming: park the task (`Poll::Pending`, no wake).
    Idle,
}

#[automock]
pub trait SubscriberPolicy<T: CommData + Debug + 'static>: Send + 'static {
    /// Called by `Subscriber::subscribe`; assert `max_num_samples` here / inject failures.
    fn subscribe(&self, max_num_samples: usize) -> Result<()>;
}

#[automock]
pub trait SubscriptionPolicy<T: CommData + Debug + 'static>: Send + 'static {
    /// Single source of truth for every receive path (`try_receive`, `receive`,
    /// `cancellable_receive`, `to_stream`).
    fn poll_samples(&self, max_samples: usize) -> Result<PollOutcome<T>>;
    /// Called by `unsubscribe`.
    fn unsubscribe(&self);
}
```

**Do not** let the wrapper self-wake unconditionally — that busy-spins a tokio worker. `Retry`
(re-poll) vs `Idle` (park) is what makes "5 canned samples then silence" behave like the real
backend under `tokio::time::timeout`.

### 5.3 `MockSubscriberImpl`

```rust
pub struct MockSubscriberImpl<T: CommData + Debug> {
    identifier: &'static str,
    info: MockConsumerInfo,
    policy: Arc<Mutex<MockSubscriberPolicy<T>>>,
}

impl<T: CommData + Debug> Subscriber<T, MockRuntimeImpl> for MockSubscriberImpl<T> {
    type Subscription = MockSubscriptionImpl<T>;

    fn new(identifier: &'static str, instance_info: MockConsumerInfo) -> Result<Self> {
        // registry lookup, same shape as MockPublisherImpl::new
    }

    fn subscribe(self, max_num_samples: usize) -> Result<Self::Subscription> {
        self.policy.lock().expect("poisoned").subscribe(max_num_samples)?;
        let sub_policy = self.info.registry
            .take_subscription::<T>(&self.info.instance, self.identifier)
            .unwrap_or_else(|| panic!(…));
        Ok(MockSubscriptionImpl::new(self.identifier, self.info, max_num_samples, sub_policy))
    }
}
```

### 5.4 `MockSubscriptionImpl` — the four receive paths

```rust
pub struct MockSubscriptionImpl<T: CommData + Debug> {
    identifier: &'static str,
    info: MockConsumerInfo,
    max_num_samples: usize,
    policy: Arc<Mutex<MockSubscriptionPolicy<T>>>,
    buffered: Mutex<VecDeque<T>>,   // leftovers from a batch, used by to_stream
}

impl<T: CommData + Debug> Subscription<T, MockRuntimeImpl> for MockSubscriptionImpl<T> {
    type Subscriber = MockSubscriberImpl<T>;
    type Sample<'a> = MockSample<T> where Self: 'a;
```

Implement, in this order:

1. **`unsubscribe(self) -> Self::Subscriber`** — call `policy.unsubscribe()`, then rebuild the
   subscriber (re-take the subscriber policy from the registry, or keep the `Arc` you already
   own — keeping it is simpler and preserves expectations).
2. **`try_receive<'a>(&'a self, scratch: &mut SampleContainer<Self::Sample<'a>>, max_samples)
   -> Result<usize>`**
   * `max_samples == 0` ⇒ `Err(Error::ReceiveError(...))` (see `error.rs` for the variant; mirror
     what `com-api-runtime-lola/consumer.rs::try_receive_samples` returns).
   * Call `policy.poll_samples(max_samples)?`; on `Retry`/`Idle`/empty ⇒ `Ok(0)`.
   * Honour `SampleContainer`'s replacement semantics: while
     `scratch.sample_count() + new >= max_samples` (or the container is full) `pop_front()` before
     `push_back()`. `SampleContainer::push_back` returns
     `Err(Error::AllocateError(OutOfMemory))` when full — never let that escape as a spurious
     error.
   * Return the number of **newly added** samples.
3. **`cancellable_receive<'a>(…, cancellation: impl Future<Output=()> + Send + 'static)
   -> impl Future<Output=(SampleContainer<Self::Sample<'a>>, Result<usize>)> + 'a`**
   * Hand-write a `ReceiveFuture` (see `com-api-runtime-lola/consumer.rs:721` for the shape), or
     compose with `futures::future::poll_fn`.
   * Each poll: `poll_samples` → push into the owned `SampleContainer`; resolve once
     `new >= new_samples`; on `Retry` `cx.waker().wake_by_ref()` and return `Pending`; on `Idle`
     return `Pending` **without** waking; poll `cancellation` first and resolve with whatever was
     collected when it fires.
   * The container must come back **in every case** (success, error, cancellation) — that is a
     documented contract of the trait.
   * `receive` has a default body delegating here; do not re-implement it.
4. **`to_stream<'a>(&'a mut self) -> impl Stream<Item = Result<Self::Sample<'a>>> + Unpin + 'a`**
   * `futures::stream::poll_fn`, wrapped so it is `Unpin` (a `poll_fn` stream over a closure is
     `Unpin` if the closure is; add `Box::pin` only if the compiler insists — `Box::pin(..)` is
     `Unpin`).
   * Drain `buffered` one sample at a time; when empty, call `poll_samples(max_num_samples)`;
     map `Retry` → wake + `Pending`, `Idle` → `Pending`, `Err(e)` → `Poll::Ready(Some(Err(e)))`
     (the stream must keep going after an error, per the trait docs).

---

## 6. `service_discovery.rs` and `producer_builder.rs`

These traits have no GATs, so try mockall first.

### 6.1 `MockServiceDiscoveryImpl<I>` — `mock!`

```rust
mockall::mock! {
    pub ServiceDiscoveryImpl<I: Interface + Send + 'static> {}

    impl<I: Interface + Send + 'static> ServiceDiscovery<I, MockRuntimeImpl>
        for ServiceDiscoveryImpl<I>
    {
        type ConsumerBuilder = MockConsumerBuilderImpl<I>;
        type ServiceEnumerator = Vec<MockConsumerBuilderImpl<I>>;

        fn get_available_instances(&self) -> Result<Vec<MockConsumerBuilderImpl<I>>>;
        fn get_available_instances_async(&self)
            -> impl Future<Output = Result<Vec<MockConsumerBuilderImpl<I>>>> + Send;
    }
}
```

Facts you can rely on (verified against the vendored mockall 0.14.0 sources):
* Associated types **in `mock!` must be fully specified** inside the impl block (mockall_derive
  emits `"associated types in mock! must be fully specified"` otherwise). With `#[automock]` the
  equivalent is the metaitem form `#[automock(type ConsumerBuilder = …;)]`.
* `-> impl Future<Output = X> + Send` is supported: mockall rewrites the expectation's return type
  to `Pin<Box<dyn Future<Output = X> + Send>>`, so tests write
  `.returning(|| Box::pin(async { Ok(vec![builder]) }))`.
* Methods taking `self` by value are supported; use `.return_once(…)`.
* Methods returning `&T` are supported via `.return_const(value)`.

### 6.2 `MockConsumerBuilderImpl<I>` — trait combo, `mock!`

```rust
mockall::mock! {
    pub ConsumerBuilderImpl<I: Interface + 'static> {}

    impl<I: Interface + 'static> ConsumerDescriptor<MockRuntimeImpl> for ConsumerBuilderImpl<I> {
        fn get_instance_identifier(&self) -> &InstanceSpecifier;
    }
    impl<I: Interface + 'static> Builder<I::Consumer<MockRuntimeImpl>> for ConsumerBuilderImpl<I> {
        fn build(self) -> Result<I::Consumer<MockRuntimeImpl>>;
    }
    impl<I: Interface + 'static> ConsumerBuilder<I, MockRuntimeImpl> for ConsumerBuilderImpl<I> {}
}
```

⚠ **Risk / contingency.** `I::Consumer<MockRuntimeImpl>` is itself a *GAT projection* of
`Interface`. If mockall's type rewriting chokes on it (compile error inside the macro expansion),
fall back to **hand-written builder + policy**, which is barely more code:

```rust
pub struct MockConsumerBuilderImpl<I: Interface> {
    instance: InstanceSpecifier,
    registry: MockRegistry,
    policy: Arc<Mutex<MockBuilderPolicy>>,   // #[automock] trait { fn build(&self) -> Result<()>; }
    _i: PhantomData<I>,
}
impl<I: Interface> Builder<I::Consumer<MockRuntimeImpl>> for MockConsumerBuilderImpl<I> {
    fn build(self) -> Result<I::Consumer<MockRuntimeImpl>> {
        self.policy.lock().expect("poisoned").build()?;      // scriptable failure injection
        Ok(Consumer::new(MockConsumerInfo { instance: self.instance, registry: self.registry }))
    }
}
```

Decide this **early** with a 10-line smoke crate; do not discover it at the end.

### 6.3 `MockProducerBuilderImpl<I>`

Same shape (`Builder<I::Producer<MockRuntimeImpl>>` + the marker trait
`ProducerBuilder<I, MockRuntimeImpl>`), with the same fallback. Its `build()` must produce
`I::Producer<MockRuntimeImpl>` via `Producer::new(ProviderInfoHandle { … })`, because the macro's
`offer()` will later clone that handle into every `Publisher::new` call and invoke
`offer_service()` on it.

---

## 7. `runtime.rs`

```rust
#[derive(Clone, Default)]
pub struct MockRuntimeImpl { registry: MockRegistry }

impl MockRuntimeImpl {
    pub fn new() -> Self { Self::default() }
    pub fn registry(&self) -> &MockRegistry { &self.registry }

    /// Pre-load the mock that the next `find_service::<I>(spec)` shall return.
    pub fn expect_find_service<I: Interface + Send + 'static>(
        &self, instance: &InstanceSpecifier, mock: MockServiceDiscoveryImpl<I>) { … }
    /// Pre-load the mock that the next `producer_builder::<I>(spec)` shall return.
    pub fn expect_producer_builder<I: Interface + 'static>(
        &self, instance: &InstanceSpecifier, mock: MockProducerBuilderImpl<I>) { … }
    /// Pre-load the `ProviderInfo` policy used by producers of this instance.
    pub fn expect_provider_info(&self, instance: &InstanceSpecifier,
                                policy: MockProviderInfoPolicy) { … }
}

impl Runtime for MockRuntimeImpl {
    type ServiceDiscovery<I: Interface + Send> = MockServiceDiscoveryImpl<I>;
    type Subscriber<T: CommData + Debug>      = MockSubscriberImpl<T>;
    type ProducerBuilder<I: Interface>        = MockProducerBuilderImpl<I>;
    type Publisher<T: CommData + Debug>       = MockPublisherImpl<T>;
    type ProviderInfo                         = ProviderInfoHandle;
    type ConsumerInfo                         = MockConsumerInfo;

    fn find_service<I: Interface + Send>(&self, spec: FindServiceSpecifier)
        -> Self::ServiceDiscovery<I> { /* take from registry, else fresh unconfigured mock */ }

    fn producer_builder<I: Interface>(&self, spec: InstanceSpecifier)
        -> Self::ProducerBuilder<I> { /* idem */ }
}
```

* `find_service` takes `FindServiceSpecifier` (`Specific(InstanceSpecifier)` | `Any`); key the
  registry on the specific instance and handle `Any` with a dedicated `"*"` key.
* Returning a **fresh unconfigured** mock on a miss is the documented Plan B behavior: the test
  that calls `find_service` directly can script the returned object in place; the test that goes
  through a helper pre-loads it via `expect_find_service`.
* Add `MockRuntimeBuilderImpl` implementing `Builder<MockRuntimeImpl>` + `RuntimeBuilder` with a
  no-op `load_config` (copy the shape from `com-api-runtime-mock/runtime.rs`), so the mock runtime
  is a drop-in for `LolaRuntimeBuilderImpl`.
* `MockRuntimeImpl` must be `Send + Sync` (the mirrored test shares it across tokio tasks) — it is,
  because everything scriptable lives behind `Arc<Mutex<…>>`.

---

## 8. Crate unit tests (`#[cfg(test)] mod test` in each file)

Minimum set, all inside the crate (wired by the `rust_test` target from step 1):

| Test | Asserts |
|---|---|
| `provider_info_offer_is_scripted` | `expect_offer_service().times(1)`, clone shares expectations |
| `publisher_allocate_error_is_injected` | `expect_allocate().returning(\|\| Err(...))` surfaces from `allocate()` |
| `publisher_send_matches_argument` | `expect_send().with(eq(Tire{…})).times(1)` via `write(..).send()` **and** via `Publisher::send` |
| `try_receive_respects_max_samples` | container replacement semantics + `Ok(0)` on `Retry`/`Idle` |
| `try_receive_zero_max_samples_errors` | error path |
| `cancellable_receive_returns_container_on_cancel` | container always returned |
| `to_stream_yields_then_parks` | `Ready(vec![a,b])` then `Idle` ⇒ two items then pending |
| `subscribe_forwards_max_num_samples` | `expect_subscribe().with(eq(5))` |
| `registry_miss_panics_with_context` | panic message names instance + event |

Define a local `#[derive(Debug, Reloc, CommData)] #[repr(C)] struct TestData { … }` for these (see
`com-api-runtime-lola/producer.rs:540` for the pattern).

---

## 9. Re-export shim `score_com_mockall`

New file `score/mw/com/rust/score_com_mockall.rs`:

```rust
//! Test-only re-export of the full COM API including the mockall runtime.
//! Depend on `//score/mw/com/rust:score_com_mockall` (instead of `:score_com`).
pub use score_com::*;
pub use com_api_runtime_mockall::{
    MockConsumerInfo, MockProviderInfoPolicy, MockPublisherPolicy, MockRegistry,
    MockRuntimeBuilderImpl, MockRuntimeImpl, MockServiceDiscoveryImpl, MockSubscriberPolicy,
    MockSubscriptionPolicy, PollOutcome, ProviderInfoHandle, /* … */
};
```

In `score/mw/com/rust/BUILD`, next to `score_com_mock`:

```python
rust_library(
    name = "score_com_mockall",
    testonly = True,
    srcs = ["score_com_mockall.rs"],
    crate_name = "score_com",          # required: interface!-generated code says `score_com::…`
    visibility = ["//visibility:public"],
    deps = [
        ":score_com",
        "//score/mw/com/impl/rust/com-api/com-api-runtime-mockall",
    ],
)
```

`crate_name = "score_com"` is mandatory and is exactly what `score_com_mock` already does — the
`interface!` macro expands to absolute `score_com::` paths. Leave `score_com_mock` /
`com-api-runtime-mock` alone.

---

## 10. Mirrored integration test

New file `score/mw/com/example/com-api-example/tests_using_mockall_runtime.rs`, mirroring
`stream_and_send_using_multi_thread` but with every interaction scripted. Skeleton:

```rust
#[cfg(test)]
mod test {
    use score_com::{/* concept traits */ Builder, FindServiceSpecifier, InstanceSpecifier,
                    OfferedProducer, Producer, Publisher, Runtime, SampleContainer,
                    SampleMaybeUninit, SampleMut, ServiceDiscovery, Subscriber, Subscription,
                    /* mockall runtime */ MockRuntimeImpl, MockPublisherPolicy,
                    MockSubscriptionPolicy, MockProviderInfoPolicy, PollOutcome};
    use com_api_gen::{Tire, VehicleInterface, VehicleOfferedProducer};
    use mockall::predicate::*;
    use futures::stream::StreamExt;

    const PRODUCER_ITERATIONS: usize = 5;
    const INITIAL_TIRE_PRESSURE: f32 = 1.0;

    fn expected_tires() -> Vec<Tire> {
        (0..PRODUCER_ITERATIONS)
            .map(|i| Tire { pressure: INITIAL_TIRE_PRESSURE + i as f32 })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_and_send_using_multi_thread_with_mockall() {
        let runtime = std::sync::Arc::new(MockRuntimeImpl::new());
        let spec = InstanceSpecifier::new("/Vehicle/Service1/Instance").unwrap();

        // ---- provider side expectations -------------------------------------
        let mut provider = MockProviderInfoPolicy::new();
        provider.expect_offer_service().times(1).returning(|| Ok(()));
        provider.expect_stop_offer_service().times(1).returning(|| Ok(()));
        runtime.expect_provider_info(&spec, provider);

        let mut left_tire_pub = MockPublisherPolicy::<Tire>::new();
        left_tire_pub.expect_allocate().times(PRODUCER_ITERATIONS).returning(|| Ok(()));
        for tire in expected_tires() {
            left_tire_pub.expect_send().with(eq(tire)).times(1).returning(|_| Ok(()));
        }
        runtime.registry().expect_publisher::<Tire>(&spec, "left_tire", left_tire_pub);
        // `exhaust` is created by `offer()` too — register a policy for it as well
        // (no expectations ⇒ any call panics, which is the desired strictness).

        // ---- consumer side expectations -------------------------------------
        let mut subscription = MockSubscriptionPolicy::<Tire>::new();
        let mut batch = expected_tires().into_iter();
        subscription.expect_poll_samples()
            .returning(move |_max| Ok(match batch.next() {
                Some(t) => PollOutcome::Ready(vec![t]),
                None => PollOutcome::Idle,
            }));
        subscription.expect_unsubscribe().return_const(());
        runtime.registry().expect_subscription::<Tire>(&spec, "left_tire", subscription);
        // …plus MockSubscriberPolicy with expect_subscribe().with(eq(SAMPLE_COUNT))
        // …plus MockServiceDiscoveryImpl::<VehicleInterface> whose
        //    expect_get_available_instances_async() returns Box::pin(async { Ok(vec![builder]) })
        //    and a consumer builder whose build() yields VehicleConsumer<MockRuntimeImpl>.

        // ---- exercise: same shape as the Lola test --------------------------
        // spawn create_consumer_async, spawn async_data_sender_fn,
        // subscribe, spawn stream_processor_fn, join, unoffer.
    }
}
```

Guidelines:
* Reuse `async_data_sender_fn` / `stream_processor_fn` **verbatim** (generic over `R: Runtime`) —
  that is the point of the exercise: unmodified user code runs against the mock runtime.
* Drop the `tokio::time::sleep(SAMPLE_INTERVAL_MS)` delays or shrink them to ~1 ms; there is no
  real IPC to wait for and CI time matters.
* Optionally add `mockall::Sequence` to enforce `offer_service` → `allocate`/`send`×5 →
  `stop_offer_service` (precedent: `com-api-runtime-lola/{producer,consumer}.rs`).
* Add a second, negative test (e.g. `expect_allocate()` returns
  `Err(Error::AllocateError(...))` and the sender logs and continues) to demonstrate error
  injection, which is Plan B's raison d'être.

BUILD wiring in `score/mw/com/example/com-api-example/BUILD`:

```python
rust_test(
    name = "com-api-example-mockall-runtime-test",
    srcs = ["tests_using_mockall_runtime.rs"],
    features = ["link_std_cpp_lib"],       # com-api-gen transitively links the Lola/C++ bridge
    deps = [
        "//score/mw/com/example/com-api-example/com-api-gen",
        "//score/mw/com/rust:score_com_mockall",
        "@score_communication_crate_index//:futures",
        "@score_communication_crate_index//:mockall",
        "@score_communication_crate_index//:tokio",
    ],
)
```

**No `tags = ["manual"]`, no `data = [...]`** — the test needs neither a config file nor a running
backend, so it runs in normal CI (unlike `com-api-example-tokio-integration-test`).

Note: the test target depends on `score_com_mockall` (crate name `score_com`) while `com-api-gen`
was compiled against `:score_com`. That is fine — only the direct `--extern score_com` of the test
crate is remapped, and both shims re-export the *same* `score_com_concept` items. If you later
want the mockall test to stop linking the C++ Lola bridge entirely, add a concept-only shim plus a
`testonly` `com-api-gen` variant compiled against it; that is an optional follow-up, not part of
Plan B.

---

## 11. Validation

Run in this order; each step is cheap and localizes failures:

```bash
# 1. crate compiles + its own unit tests
bazel build //score/mw/com/impl/rust/com-api/com-api-runtime-mockall/...
bazel test  //score/mw/com/impl/rust/com-api/com-api-runtime-mockall:com-api-runtime-mockall-tests

# 2. shim
bazel build //score/mw/com/rust:score_com_mockall

# 3. mirrored integration test
bazel test //score/mw/com/example/com-api-example:com-api-example-mockall-runtime-test

# 4. nothing else regressed (concept.rs is untouched, so this should be a no-op)
bazel test //score/mw/com/...
```

Also confirm: `score/mw/com/rust/score_com_concept/concept.rs` shows **no** diff, and
`com-api-runtime-mock` / `score_com_mock` are unchanged.

---

## 12. Definition of done

- [ ] `com-api-runtime-mockall` crate builds, `testonly`, depends only on `score_com_concept`,
      `futures`, `mockall`.
- [ ] All six `Runtime` associated types are populated by mock-backed types; every behavioral
      decision is reachable through a `expect_*()` call.
- [ ] Registry injection documented in the crate-level doc comment, with a copy-pasteable example.
- [ ] Unit tests from §8 green.
- [ ] `tests_using_mockall_runtime.rs` reproduces `stream_and_send_using_multi_thread` with fully
      scripted expectations and runs in normal CI (no `manual` tag).
- [ ] `concept.rs` untouched; `com-api-runtime-mock`, `score_com_mock` untouched
- [ ] Commits carry `Signed-off-by:` (DCO, see recent history) and close the upstream request:
      `Closes eclipse-score/communication#490`.

## 13. Known risks, ranked

1. **`I::Consumer<R>` / `I::Producer<R>` projections inside `mock!`** — GAT projection in a mocked
   return type. Mitigation: §6.2 fallback (hand-written builder + `MockBuilderPolicy`). Probe this
   on day one.
2. **`impl Future + Send` in `mock!`** — supported (`Pin<Box<dyn Future … + Send>>`), but if the
   `+ Send` bound is dropped by the rewrite, `get_available_instances_async` stops satisfying the
   trait. Mitigation: hand-write `MockServiceDiscoveryImpl` with a policy method returning
   `Result<Vec<MockConsumerBuilderImpl<I>>>` and wrap it in an `async` block.
3. **`Send`/`Sync` of mock objects across `tokio::spawn`** — always store mocks as
   `Arc<Mutex<Mock…>>` (the `SharedMockBridge` pattern); never as bare fields.
4. **Busy-spin in async receive paths** — guaranteed avoided only if `PollOutcome::Idle` parks
   without waking. Review every `wake_by_ref()` call site.
5. **Registry key mismatches** (event name typo ⇒ panic at construction time). Mitigation: the
   panic message must print instance, event and slot kind.
