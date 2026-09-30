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

//! In-memory pub/sub state shared by the whole fake runtime.
//!
//! Everything a [`FakeRuntimeImpl`](crate::FakeRuntimeImpl) hands out
//! (producers, providers, consumers, publishers, subscriptions) shares one
//! [`FakeRegistry`] behind `Arc`, so a `Publisher<T>` created by an offered
//! producer and a `Subscription<T>` created by a discovered consumer for the
//! *same* interface, instance and event really do exchange data. That is what
//! makes the fake backend **functional** (Plan A): samples are retained per
//! event and delivered to subscriptions, unlike the mockall backend where
//! nothing happens unless scripted.
//!
//! Data model, one level below the `interface!`-generated endpoints:
//! - an **instance** is identified by `(INTERFACE_ID, InstanceSpecifier)` and
//!   becomes visible to `find_service` only while it is *offered*
//!   ([`FakeProviderInfo::offer_service`](crate::FakeProviderInfo));
//! - every event of an instance owns one [`EventQueue<T>`], an ordered
//!   buffer of published `T` values;
//! - values are consumed (removed) when a subscription reads them; a
//!   subscriber established *after* samples were already sent still receives
//!   the not-yet-consumed samples in send order.
//!
//! Locking: the outer `instances` map is locked briefly to locate (or create)
//! an entry, then **dropped** before any per-entry work, so a waker fired by
//! one thread can re-enter the registry from another without a self-deadlock.
//! `drain_or_wait` registers a waker under the same queue lock it checks for
//! values, so a publish can never race a park and be lost.

use score_com_concept::{CommData, Error, InstanceSpecifier, ReceiveFailedReason, Result};
use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::task::Waker;

/// A type-erased [`EventQueue`] keyed by event name; the concrete `T` is
/// recovered with [`ErasedQueue::as_t`].
#[derive(Clone)]
pub(crate) struct ErasedQueue {
    pub(crate) event: String,
    inner: Arc<dyn Any + Send + Sync>,
}

impl Debug for ErasedQueue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ErasedQueue").field("event", &self.event).finish()
    }
}

impl ErasedQueue {
    fn from_typed<T: CommData + Debug>(queue: Arc<EventQueue<T>>) -> Self {
        Self {
            event: queue.event.clone(),
            inner: queue as Arc<dyn Any + Send + Sync>,
        }
    }

    /// Recover the strongly-typed queue; one event maps to exactly one `T`,
    /// so `None` is a programming error.
    pub(crate) fn as_t<T: CommData + Debug>(&self) -> Option<Arc<EventQueue<T>>> {
        self.inner.clone().downcast::<EventQueue<T>>().ok()
    }
}

/// Ordered buffer of published values for one event of one instance.
///
/// Interior mutability (`Mutex`) lets a publisher and every subscription
/// share one `Arc<EventQueue<T>>` across threads. Values are stored **by
/// value** in send order and consumed (removed) when a subscription reads
/// them; this keeps `FakeSample<T>` able to own its `T` without a `Sync`
/// bound, which the `Runtime` GAT (`type Subscriber<T: CommData + Debug>`)
/// requires. Samples are retained until a subscription consumes them, so a
/// subscriber established after samples were already sent still receives the
/// not-yet-consumed samples in send order.
pub struct EventQueue<T: CommData + Debug> {
    pub(crate) event: String,
    state: Mutex<State<T>>,
}

#[derive(Debug)]
struct State<T> {
    values: std::collections::VecDeque<T>,
    /// Wakers of tasks parked waiting for new samples on this queue.
    wakers: Vec<Waker>,
}

impl<T: CommData + Debug> Debug for EventQueue<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let guard = self.state.lock().expect("queue poisoned");
        f.debug_struct("EventQueue")
            .field("event", &self.event)
            .field("value_count", &guard.values.len())
            .finish()
    }
}

impl<T: CommData + Debug> EventQueue<T> {
    pub(crate) fn new(event: String) -> Self {
        Self {
            event,
            state: Mutex::new(State {
                values: std::collections::VecDeque::new(),
                wakers: Vec::new(),
            }),
        }
    }

    /// Append `value` to the tail of the queue and wake parked receivers.
    pub(crate) fn push(&self, value: T) {
        let mut guard = self.state.lock().expect("queue poisoned");
        guard.values.push_back(value);
        let wakers = std::mem::take(&mut guard.wakers);
        drop(guard);
        for waker in wakers {
            waker.wake();
        }
    }

    /// Number of values currently retained (not yet consumed).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.state.lock().expect("queue poisoned").values.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.state.lock().expect("queue poisoned").values.is_empty()
    }

    /// Remove and return up to `n` values from the front, in send order
    /// (non-blocking; returns an empty `Vec` when nothing is pending).
    pub(crate) fn drain_front(&self, n: usize) -> Vec<T> {
        let mut guard = self.state.lock().expect("queue poisoned");
        let take = n.min(guard.values.len());
        guard.values.drain(..take).collect()
    }

    /// Remove and return up to `n` values from the front. If the queue is
    /// empty, register `waker` **under the same lock** (preventing a lost
    /// wakeup) and return `None`.
    pub(crate) fn drain_or_wait(&self, n: usize, waker: &Waker) -> Option<Vec<T>> {
        let mut guard = self.state.lock().expect("queue poisoned");
        if guard.values.is_empty() {
            guard.wakers.push(waker.clone());
            return None;
        }
        let take = n.min(guard.values.len());
        let batch: Vec<T> = guard.values.drain(..take).collect();
        Some(batch)
    }
}

/// A service instance: its specifier and its mutable state (offered flag and
/// per-event queues). The state is behind its own mutex so the instance
/// record can be held in a shared `Vec` without needing `InstanceState:
/// Clone`.
#[derive(Debug, Clone)]
pub(crate) struct OwnedInstance {
    pub(crate) interface_id: String,
    pub(crate) instance: InstanceSpecifier,
    pub(crate) state: Arc<Mutex<InstanceState>>,
}

impl OwnedInstance {
    fn new(interface_id: &str, instance: &InstanceSpecifier) -> Self {
        Self {
            interface_id: interface_id.to_owned(),
            instance: instance.clone(),
            state: Arc::new(Mutex::new(InstanceState::default())),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct InstanceState {
    pub(crate) offered: bool,
    pub(crate) events: Vec<ErasedQueue>,
}

/// Interface-level wakers of async service discoveries. A discovery parks on
/// its interface's waiter list (regardless of `Specific`/`Any`); when any
/// instance of the interface is (re-)offered, all parked discoveries are
/// woken and re-check their own matching condition.
type InterfaceWaiters = std::collections::HashMap<String, Vec<Waker>>;

/// Shared state of one fake runtime. Cloning shares the same map, so all
/// handles handed out by any clone interoperate.
///
/// `InstanceSpecifier` does not implement `Hash`/`Eq`, so instances live in a
/// `Vec` keyed by their `(interface, specifier)` strings; linear scan is fine
/// for a test double.
#[derive(Clone)]
pub struct FakeRegistry {
    pub(crate) instances: Arc<Mutex<Vec<OwnedInstance>>>,
    pub(crate) interface_waiters: Arc<Mutex<InterfaceWaiters>>,
}

impl Debug for FakeRegistry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FakeRegistry")
            .field(
                "instance_count",
                &self.instances.lock().expect("registry poisoned").len(),
            )
            .finish()
    }
}

impl Default for FakeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeRegistry {
    pub fn new() -> Self {
        Self {
            instances: Arc::new(Mutex::new(Vec::new())),
            interface_waiters: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Locate (by string form) or lazily create the instance record for
    /// `(interface_id, instance)`, returning a shared handle to its state.
    fn state_for(
        &self,
        interface_id: &str,
        instance: &InstanceSpecifier,
    ) -> Arc<Mutex<InstanceState>> {
        let mut guard = self.instances.lock().expect("registry poisoned");
        let key = instance.as_ref();
        if let Some(entry) = guard
            .iter()
            .find(|e| e.interface_id == interface_id && e.instance.as_ref() == key)
        {
            return Arc::clone(&entry.state);
        }
        let entry = OwnedInstance::new(interface_id, instance);
        let state = Arc::clone(&entry.state);
        guard.push(entry);
        state
    }

    /// The retained queue for `(interface_id, instance, event)`, created
    /// lazily.
    pub(crate) fn queue_for<T: CommData + Debug>(
        &self,
        interface_id: &str,
        instance: &InstanceSpecifier,
        event: &str,
    ) -> Arc<EventQueue<T>> {
        let state = self.state_for(interface_id, instance);
        let mut guard = state.lock().expect("state poisoned");
        if let Some(index) = guard.events.iter().position(|e| e.event == event) {
            let queue = guard.events[index].as_t::<T>().unwrap_or_else(|| {
                panic!(
                    "event '{}' was registered with a different value type",
                    event
                )
            });
            drop(guard);
            queue
        } else {
            let queue = Arc::new(EventQueue::<T>::new(event.to_owned()));
            guard.events.push(ErasedQueue::from_typed(queue.clone()));
            drop(guard);
            queue
        }
    }

    /// Mark `(interface_id, instance)` as offered and wake waiting
    /// discoveries.
    pub(crate) fn set_offered(
        &self,
        interface_id: &str,
        instance: &InstanceSpecifier,
        offered: bool,
    ) {
        let state = self.state_for(interface_id, instance);
        {
            let mut guard = state.lock().expect("state poisoned");
            guard.offered = offered;
        }
        self.wake_offer_waiters(interface_id);
    }

    /// `true` if the instance is currently offered.
    #[cfg(test)]
    pub(crate) fn is_offered(&self, interface_id: &str, instance: &InstanceSpecifier) -> bool {
        let state = self.state_for(interface_id, instance);
        let guard = state.lock().expect("state poisoned");
        let offered = guard.offered;
        drop(guard);
        offered
    }

    /// Snapshot of the offered instances of `interface_id`, ordered by
    /// registration.
    pub(crate) fn offered_instances(&self, interface_id: &str) -> Vec<InstanceSpecifier> {
        let guard = self.instances.lock().expect("registry poisoned");
        let offered: Vec<InstanceSpecifier> = guard
            .iter()
            .filter(|entry| entry.interface_id == interface_id)
            .filter(|entry| entry.state.lock().expect("state poisoned").offered)
            .map(|entry| entry.instance.clone())
            .collect();
        drop(guard);
        offered
    }

    /// Register a waker that wants to be told when any instance of
    /// `interface_id` is (re-)offered.
    pub(crate) fn add_interface_waiter(&self, interface_id: &str, waker: Waker) {
        self.interface_waiters
            .lock()
            .expect("registry poisoned")
            .entry(interface_id.to_owned())
            .or_default()
            .push(waker);
    }

    /// Wake every discovery waiting for `interface_id` to appear.
    pub(crate) fn wake_offer_waiters(&self, interface_id: &str) {
        let waiters: Vec<Waker> = {
            let mut guard = self.interface_waiters.lock().expect("registry poisoned");
            guard
                .remove(interface_id)
                .unwrap_or_default()
        };
        for waker in waiters {
            waker.wake();
        }
    }

    #[cfg(test)]
    pub(crate) fn instance_count(&self) -> usize {
        self.instances.lock().expect("registry poisoned").len()
    }
}

/// Validate the `new_samples`/`max_samples` pair against the subscription's
/// capacity (same semantics as the real Lola runtime).
pub(crate) fn validate_bounds(
    max_num_samples: usize,
    new_samples: usize,
    max_samples: usize,
) -> Result<()> {
    if new_samples == 0 || max_samples == 0 {
        return Err(Error::ReceiveError(ReceiveFailedReason::InputValueOutOfBounds {
            max: max_num_samples,
            requested: 0,
        }));
    }
    if max_samples > max_num_samples {
        return Err(Error::ReceiveError(ReceiveFailedReason::SampleCountOutOfBounds {
            max: max_num_samples,
            requested: max_samples,
        }));
    }
    if new_samples > max_samples {
        return Err(Error::ReceiveError(ReceiveFailedReason::SampleCountOutOfBounds {
            max: max_num_samples,
            requested: new_samples,
        }));
    }
    Ok(())
}

/// Validate `max_samples` against the subscription's capacity.
pub(crate) fn validate_max_samples(max_num_samples: usize, max_samples: usize) -> Result<()> {
    if max_samples == 0 {
        return Err(Error::ReceiveError(ReceiveFailedReason::InputValueOutOfBounds {
            max: max_num_samples,
            requested: 0,
        }));
    }
    if max_samples > max_num_samples {
        return Err(Error::ReceiveError(ReceiveFailedReason::SampleCountOutOfBounds {
            max: max_num_samples,
            requested: max_samples,
        }));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    #[repr(C)]
    struct TestData {
        value: i32,
    }

    unsafe impl score_com_concept::Reloc for TestData {}

    impl score_com_concept::CommData for TestData {
        const ID: &'static str = "TestData";
    }

    #[test]
    fn queue_push_and_drain_front() {
        let queue = EventQueue::<TestData>::new("evt".into());
        assert!(queue.is_empty());
        queue.push(TestData { value: 1 });
        queue.push(TestData { value: 2 });
        queue.push(TestData { value: 3 });
        assert_eq!(queue.len(), 3);
        let two = queue.drain_front(2);
        assert_eq!(two, vec![TestData { value: 1 }, TestData { value: 2 }]);
        // Only the last remains.
        assert_eq!(queue.drain_front(10), vec![TestData { value: 3 }]);
        assert!(queue.is_empty());
    }

    #[test]
    fn queue_drain_or_wait_parks_when_empty() {
        let queue = EventQueue::<TestData>::new("evt".into());
        // A no-op waker; the point is that None is returned and a waker is
        // registered (we cannot easily assert the registration count, but the
        // None result proves the park path).
        let waker = futures::task::noop_waker();
        let got = queue.drain_or_wait(5, &waker);
        assert!(got.is_none());
        // After a push, the same call returns the value.
        queue.push(TestData { value: 9 });
        assert_eq!(queue.drain_or_wait(5, &waker), Some(vec![TestData { value: 9 }]));
    }

    #[test]
    fn clone_registry_shares_state() {
        let a = FakeRegistry::new();
        let b = a.clone();
        let spec = InstanceSpecifier::new("/T/S/I").expect("valid");
        b.set_offered("Iface", &spec, true);
        assert!(a.is_offered("Iface", &spec));
        // `InstanceSpecifier` is not `PartialEq`, so compare by string form.
        let offered = a.offered_instances("Iface");
        assert_eq!(offered.len(), 1);
        assert_eq!(offered[0].as_ref(), spec.as_ref());
        b.set_offered("Iface", &spec, false);
        assert!(!a.is_offered("Iface", &spec));
        assert!(a.offered_instances("Iface").is_empty());
    }

    #[test]
    fn queue_for_is_lazily_created_and_shared() {
        let registry = FakeRegistry::new();
        let spec = InstanceSpecifier::new("/T/S/I").expect("valid");
        let q1 = registry.queue_for::<TestData>("Iface", &spec, "evt");
        q1.push(TestData { value: 7 });
        // A fresh handle for the same endpoint must see the same queue.
        let q2 = registry.queue_for::<TestData>("Iface", &spec, "evt");
        assert_eq!(
            q2.drain_front(1),
            vec![TestData { value: 7 }]
        );
        // A different event is a different queue.
        let other = registry.queue_for::<TestData>("Iface", &spec, "other");
        assert!(other.is_empty());
        // A different instance is a different queue.
        let spec2 = InstanceSpecifier::new("/T/S/Other").expect("valid");
        let other_instance = registry.queue_for::<TestData>("Iface", &spec2, "evt");
        assert!(other_instance.is_empty());
        // Two distinct instances were lazily registered.
        assert_eq!(registry.instance_count(), 2);
    }

    #[test]
    #[should_panic(expected = "registered with a different value type")]
    fn queue_for_type_mismatch_panics() {
        #[derive(Debug)]
        struct Other;
        unsafe impl score_com_concept::Reloc for Other {}
        impl score_com_concept::CommData for Other {
            const ID: &'static str = "Other";
        }

        let registry = FakeRegistry::new();
        let spec = InstanceSpecifier::new("/T/S/I").expect("valid");
        // First register the event as TestData…
        registry.queue_for::<TestData>("Iface", &spec, "evt").push(TestData { value: 1 });
        // …then asking for the same event as a different type panics.
        let _ = registry.queue_for::<Other>("Iface", &spec, "evt");
    }

    #[test]
    fn validate_bounds_rejects_zero_and_out_of_range() {
        assert!(validate_bounds(5, 3, 5).is_ok());
        assert!(validate_bounds(5, 0, 5).is_err());
        assert!(validate_bounds(5, 3, 0).is_err());
        assert!(validate_bounds(5, 3, 6).is_err()); // max > capacity
        assert!(validate_bounds(5, 6, 6).is_err()); // new > max
    }

    #[test]
    fn validate_max_samples_rejects_zero_and_over_capacity() {
        assert!(validate_max_samples(5, 5).is_ok());
        assert!(validate_max_samples(5, 0).is_err());
        assert!(validate_max_samples(5, 6).is_err());
    }
}
