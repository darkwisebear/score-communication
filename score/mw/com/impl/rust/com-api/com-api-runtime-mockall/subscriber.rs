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

//! Subscriber + Subscription GAT workaround (Plan B, Problem 1, wall #2).
//!
//! mockall 0.14 cannot `#[automock]` a trait with a generic associated type, so
//! `Subscription::Sample<'a>` cannot live inside a mock. The scriptable
//! decisions therefore live on the lifetime-free [`SubscriberPolicy`] and
//! [`SubscriptionPolicy`]; the hand-written types in this module are the only
//! place that names `Self::Sample<'a>`.
//!
//! Every receive path (`try_receive`, `receive`, `cancellable_receive`,
//! `to_stream`) funnels into [`SubscriptionPolicy::poll_samples`], whose
//! [`PollOutcome`] keeps the async paths deterministic and spin-free:
//! `Ready` delivers, `Retry` re-polls (the task wakes itself, so script only
//! *bounded* retry sequences), `Idle` parks without waking.

use crate::registry::{lookup, MockConsumerInfo};
use crate::runtime::MockRuntimeImpl;
use core::cmp::Ordering;
use core::fmt::{self, Debug};
use core::future::Future;
use core::ops::Deref;
use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use core::task::{Context, Poll};
use futures::future::poll_fn as poll_future;
use futures::stream::{poll_fn, Stream};
use mockall::automock;
use score_com_concept::{
    CommData, Error, EventFailedReason, ReceiveFailedReason, Result, Sample, SampleContainer,
    Subscription, Subscriber,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::task::Waker;

#[cfg(test)]
mod test {
    use super::*;
    use crate::registry::{MockConsumerInfo, MockRegistry};
    use futures::stream::Stream;
    use mockall::predicate::eq;
    use score_com_concept::InstanceSpecifier;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[derive(Debug, Clone, PartialEq, Copy)]
    #[repr(C)]
    struct TestData {
        v: i32,
    }

    unsafe impl score_com_concept::Reloc for TestData {}
    impl CommData for TestData {
        const ID: &'static str = "TestData";
    }

    fn test_info() -> (MockConsumerInfo, MockRegistry, InstanceSpecifier) {
        let registry = MockRegistry::new();
        let spec =
            InstanceSpecifier::new("/Test/Service1/Instance").expect("valid instance specifier");
        (
            MockConsumerInfo::new(spec.clone(), registry.clone()),
            registry,
            spec,
        )
    }

    #[test]
    fn try_receive_delivers_scripted_batch() {
        let (info, _registry, _spec) = test_info();
        let mut policy = MockSubscriptionPolicy::<TestData>::new();
        policy
            .expect_poll_samples()
            .returning(|_| Ok(PollOutcome::Ready(vec![TestData { v: 1 }, TestData { v: 2 }])));
        let sub =
            MockSubscriptionImpl::<TestData>::from_policy(info, "ev", 5, policy, MockSubscriberPolicy::<TestData>::new());
        let mut buffer = SampleContainer::new(5);
        let added = sub.try_receive(&mut buffer, 5).expect("try_receive");
        assert_eq!(added, 2);
        assert_eq!(buffer.sample_count(), 2);
    }

    #[test]
    fn try_receive_respects_max_samples_replacement() {
        let (info, _registry, _spec) = test_info();
        let mut policy = MockSubscriptionPolicy::<TestData>::new();
        // Deliver three samples but only two fit; the oldest is replaced.
        policy
            .expect_poll_samples()
            .returning(|_| {
                Ok(PollOutcome::Ready(vec![
                    TestData { v: 1 },
                    TestData { v: 2 },
                    TestData { v: 3 },
                ]))
            });
        let sub =
            MockSubscriptionImpl::<TestData>::from_policy(info, "ev", 5, policy, MockSubscriberPolicy::<TestData>::new());
        let mut buffer = SampleContainer::new(5);
        let added = sub.try_receive(&mut buffer, 2).expect("try_receive");
        assert_eq!(added, 3);
        // Only the newest two survive the replacement semantics.
        let values: Vec<i32> = buffer
            .iter()
            .map(|s| s.v)
            .collect();
        assert_eq!(values, vec![2, 3]);
    }

    #[test]
    fn try_receive_idle_or_retry_reports_zero() {
        let (info, _registry, _spec) = test_info();
        let mut policy = MockSubscriptionPolicy::<TestData>::new();
        policy
            .expect_poll_samples()
            .returning(|_| Ok(PollOutcome::Idle));
        let sub =
            MockSubscriptionImpl::<TestData>::from_policy(info, "ev", 5, policy, MockSubscriberPolicy::<TestData>::new());
        let mut buffer = SampleContainer::new(5);
        assert_eq!(sub.try_receive(&mut buffer, 5).expect("idle"), 0);

        // A scripted error surfaces through the receive path.
        let (info, _r, _s) = test_info();
        let mut err_policy = MockSubscriptionPolicy::<TestData>::new();
        err_policy
            .expect_poll_samples()
            .returning(|_| Err(Error::ReceiveError(ReceiveFailedReason::BufferUnavailable)));
        let err_sub = MockSubscriptionImpl::<TestData>::from_policy(
            info,
            "ev",
            5,
            err_policy,
            MockSubscriberPolicy::<TestData>::new(),
        );
        let mut buffer2 = SampleContainer::new(5);
        assert!(err_sub
            .try_receive(&mut buffer2, 5)
            .is_err());
    }

    #[test]
    fn try_receive_zero_max_samples_errors() {
        let (info, _registry, _spec) = test_info();
        let mut policy = MockSubscriptionPolicy::<TestData>::new();
        policy
            .expect_poll_samples()
            .returning(|_| Ok(PollOutcome::Idle));
        let sub =
            MockSubscriptionImpl::<TestData>::from_policy(info, "ev", 5, policy, MockSubscriberPolicy::<TestData>::new());
        let mut buffer = SampleContainer::new(5);
        assert!(sub.try_receive(&mut buffer, 0).is_err());
    }

    #[test]
    fn cancellable_receive_returns_container_on_cancel() {
        let (info, _registry, _spec) = test_info();
        let mut policy = MockSubscriptionPolicy::<TestData>::new();
        // Even though data is available, cancellation wins on the first poll.
        policy
            .expect_poll_samples()
            .returning(|_| Ok(PollOutcome::Ready(vec![TestData { v: 9 }])));
        let sub =
            MockSubscriptionImpl::<TestData>::from_policy(info, "ev", 5, policy, MockSubscriberPolicy::<TestData>::new());
        let buffer = SampleContainer::new(5);
        // `ready(())` is an immediately-ready, `Send + 'static` cancellation.
        // `Box::pin` so we can poll it regardless of the opaque future's `Unpin`.
        let mut fut = Box::pin(sub.cancellable_receive(buffer, 1, 5, futures::future::ready(())));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready((container, res)) => {
                assert!(container.sample_count() == 0, "container must be returned empty");
                assert!(matches!(
                    res,
                    Err(Error::ReceiveError(ReceiveFailedReason::Cancelled))
                ));
            }
            Poll::Pending => panic!("cancellation should resolve on the first poll"),
        }
    }

    #[test]
    fn to_stream_yields_then_parks() {
        let (info, _registry, _spec) = test_info();
        let mut policy = MockSubscriptionPolicy::<TestData>::new();
        // First poll delivers two samples; every later poll parks (`Idle`).
        let polls = Arc::new(Mutex::new(0usize));
        let counter = Arc::clone(&polls);
        policy.expect_poll_samples().returning(move |_max| {
            let i = {
                let mut g = counter.lock().expect("poisoned");
                let i = *g;
                *g += 1;
                i
            };
            Ok(match i {
                0 => PollOutcome::Ready(vec![TestData { v: 1 }, TestData { v: 2 }]),
                _ => PollOutcome::Idle,
            })
        });
        let sub =
            MockSubscriptionImpl::<TestData>::from_policy(info, "ev", 5, policy, MockSubscriberPolicy::<TestData>::new());
        let mut sub = sub;
        let mut stream = sub.to_stream();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        // `to_stream` is `Unpin`, so it can be polled directly.
        match Stream::poll_next(Pin::new(&mut stream), &mut cx) {
            Poll::Ready(Some(Ok(sample))) => assert_eq!(sample.v, 1),
            other => panic!("expected first sample, got {other:?}"),
        }
        match Stream::poll_next(Pin::new(&mut stream), &mut cx) {
            Poll::Ready(Some(Ok(sample))) => assert_eq!(sample.v, 2),
            other => panic!("expected second sample, got {other:?}"),
        }
        // No more scripted data: the stream parks.
        assert!(matches!(
            Stream::poll_next(Pin::new(&mut stream), &mut cx),
            Poll::Pending
        ));
    }

    #[test]
    fn subscribe_forwards_max_num_samples() {
        let (info, registry, spec) = test_info();
        let mut subscriber_policy = MockSubscriberPolicy::<TestData>::new();
        subscriber_policy
            .expect_subscribe()
            .with(eq(5))
            .times(1)
            .returning(move |max| {
                assert_eq!(max, 5);
                Ok(())
            });
        // `subscribe` pops the subscription policy from the registry.
        let mut subscription_policy = MockSubscriptionPolicy::<TestData>::new();
        subscription_policy
            .expect_poll_samples()
            .returning(|_| Ok(PollOutcome::Idle));
        registry.expect_subscription(&spec, "ev", subscription_policy);
        let subscriber =
            MockSubscriberImpl::<TestData>::from_policy(info, "ev", subscriber_policy);
        let _subscription = subscriber.subscribe(5).expect("subscribe");
        // mockall verifies `times(1)` with `eq(5)` on drop.
    }
}

/// Outcome of one scripted poll; the single source of truth for all receive
/// paths of [`MockSubscriptionImpl`].
#[derive(Debug)]
pub enum PollOutcome<T> {
    /// Deliver these samples now (may be empty ⇒ treated as "no new samples").
    Ready(Vec<T>),
    /// Nothing yet, but more is expected: poll again (the task wakes itself).
    Retry,
    /// Nothing, and nothing more is coming: park without waking.
    Idle,
}

impl<T: PartialEq> PartialEq for PollOutcome<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Ready(a), Self::Ready(b)) => a == b,
            (Self::Retry, Self::Retry) | (Self::Idle, Self::Idle) => true,
            _ => false,
        }
    }
}

/// Scriptable decisions of [`MockSubscriberImpl`].
#[automock]
pub trait SubscriberPolicy<T: CommData + Debug + 'static>: Send + 'static {
    /// Called by `Subscriber::subscribe`; assert `max_num_samples` here and/or
    /// inject a subscription failure.
    fn subscribe(&self, max_num_samples: usize) -> Result<()>;
}

/// Scriptable decisions of [`MockSubscriptionImpl`].
#[automock]
pub trait SubscriptionPolicy<T: CommData + Debug + 'static>: Send + 'static {
    /// Called by every receive path (`try_receive`, `receive`,
    /// `cancellable_receive`, `to_stream`) with the call's `max_samples`.
    fn poll_samples(&self, max_samples: usize) -> Result<PollOutcome<T>>;
    /// Called by `Subscription::unsubscribe`.
    fn unsubscribe(&self);
}

/// Process-wide reception order; [`MockSample`] compares by this so that
/// `PartialOrd`/`Ord` reflect reception order, as the trait docs require.
static SAMPLE_ORDER: AtomicUsize = AtomicUsize::new(0);

/// Hand-written sample for received data. No scriptable behavior; it exists
/// only to satisfy the `Sample` supertrait bounds (`Deref`, `Ord`, …).
pub struct MockSample<T: CommData + Debug> {
    order: usize,
    data: T,
}

impl<T: CommData + Debug> MockSample<T> {
    fn new(data: T) -> Self {
        Self {
            order: SAMPLE_ORDER.fetch_add(1, AtomicOrdering::SeqCst),
            data,
        }
    }
}

impl<T: CommData + Debug> Deref for MockSample<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T: CommData + Debug> Debug for MockSample<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockSample").field("data", &self.data).finish()
    }
}

impl<T: CommData + Debug> PartialOrd for MockSample<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: CommData + Debug> Ord for MockSample<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order.cmp(&other.order)
    }
}

impl<T: CommData + Debug> PartialEq for MockSample<T> {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }
}

impl<T: CommData + Debug> Eq for MockSample<T> {}

impl<T: CommData + Debug> Sample<T> for MockSample<T> {}

/// Hand-written `Subscriber` wrapper; the scriptable part is the
/// `Arc<Mutex<MockSubscriberPolicy<T>>>`. [`MockSubscriberImpl::new`] pops the
/// policy from the registry (panicking with context on a miss).
pub struct MockSubscriberImpl<T: CommData + Debug> {
    info: MockConsumerInfo,
    identifier: &'static str,
    policy: Arc<Mutex<MockSubscriberPolicy<T>>>,
}

impl<T: CommData + Debug> MockSubscriberImpl<T> {
    /// Test-only constructor: skip the registry and wrap an already-scripted
    /// policy. The subscription slot is still looked up on `subscribe`.
    pub fn from_policy(
        info: MockConsumerInfo,
        identifier: &'static str,
        policy: MockSubscriberPolicy<T>,
    ) -> Self {
        Self {
            info,
            identifier,
            policy: Arc::new(Mutex::new(policy)),
        }
    }

    /// Test-only constructor from a shared policy handle.
    pub fn from_shared(
        info: MockConsumerInfo,
        identifier: &'static str,
        policy: Arc<Mutex<MockSubscriberPolicy<T>>>,
    ) -> Self {
        Self {
            info,
            identifier,
            policy,
        }
    }
}

impl<T: CommData + Debug> Subscriber<T, MockRuntimeImpl> for MockSubscriberImpl<T> {
    type Subscription = MockSubscriptionImpl<T>;

    fn new(identifier: &'static str, instance_info: MockConsumerInfo) -> Result<Self> {
        let policy =
            lookup::subscriber::<T>(&instance_info.registry, &instance_info.instance, identifier)
                .unwrap_or_else(|| {
                    panic!(
                        "no MockSubscriberPolicy registered for instance '{}' event '{}'",
                        instance_info.instance.as_ref(),
                        identifier
                    )
                });
        Ok(Self {
            info: instance_info,
            identifier,
            policy,
        })
    }

    fn subscribe(self, max_num_samples: usize) -> Result<Self::Subscription> {
        self.policy.lock().expect("poisoned").subscribe(max_num_samples)?;
        let sub_policy =
            lookup::subscription::<T>(&self.info.registry, &self.info.instance, self.identifier)
                .ok_or(Error::EventError(
                    EventFailedReason::EventNotAvailable,
                ))?;
        Ok(MockSubscriptionImpl {
            info: self.info.clone(),
            identifier: self.identifier,
            max_num_samples,
            sub_policy,
            subscriber_policy: Arc::clone(&self.policy),
            buffered: Mutex::new(VecDeque::new()),
            waker: Mutex::new(None),
        })
    }
}

/// Hand-written `Subscription` wrapper. The GAT (`type Sample<'a>`) lives only
/// here; behavior is delegated to `Arc<Mutex<MockSubscriptionPolicy<T>>>`.
pub struct MockSubscriptionImpl<T: CommData + Debug> {
    info: MockConsumerInfo,
    identifier: &'static str,
    max_num_samples: usize,
    sub_policy: Arc<Mutex<MockSubscriptionPolicy<T>>>,
    /// Kept so `unsubscribe` can hand back a subscriber whose expectations
    /// are still live.
    subscriber_policy: Arc<Mutex<MockSubscriberPolicy<T>>>,
    /// Leftovers of a previous batched fetch, used by `to_stream`.
    buffered: Mutex<VecDeque<T>>,
    /// Waker of the last parked async receiver; a later delivery wakes it.
    waker: Mutex<Option<Waker>>,
}

impl<T: CommData + Debug> MockSubscriptionImpl<T> {
    /// Test-only constructor: skip the registry.
    pub fn from_policy(
        info: MockConsumerInfo,
        identifier: &'static str,
        max_num_samples: usize,
        policy: MockSubscriptionPolicy<T>,
        subscriber_policy: MockSubscriberPolicy<T>,
    ) -> Self {
        Self {
            info,
            identifier,
            max_num_samples,
            sub_policy: Arc::new(Mutex::new(policy)),
            subscriber_policy: Arc::new(Mutex::new(subscriber_policy)),
            buffered: Mutex::new(VecDeque::new()),
            waker: Mutex::new(None),
        }
    }

    /// Validates the `new_samples`/`max_samples` pair against the
    /// subscription's capacity (Lola parity).
    fn validate_bounds(&self, new_samples: usize, max_samples: usize) -> Result<()> {
        if new_samples == 0 || max_samples == 0 {
            let requested = if new_samples == 0 { 0 } else { max_samples };
            return Err(Error::ReceiveError(
                ReceiveFailedReason::InputValueOutOfBounds {
                    max: self.max_num_samples,
                    requested,
                },
            ));
        }
        if max_samples > self.max_num_samples {
            return Err(Error::ReceiveError(
                ReceiveFailedReason::SampleCountOutOfBounds {
                    max: self.max_num_samples,
                    requested: max_samples,
                },
            ));
        }
        Ok(())
    }

    /// Validates `max_samples` against the subscription's capacity.
    fn validate_max_samples(&self, max_samples: usize) -> Result<()> {
        if max_samples == 0 {
            return Err(Error::ReceiveError(
                ReceiveFailedReason::InputValueOutOfBounds {
                    max: self.max_num_samples,
                    requested: 0,
                },
            ));
        }
        if max_samples > self.max_num_samples {
            return Err(Error::ReceiveError(
                ReceiveFailedReason::SampleCountOutOfBounds {
                    max: self.max_num_samples,
                    requested: max_samples,
                },
            ));
        }
        Ok(())
    }

    /// Single source of truth: polls the policy once and applies the
    /// container replacement semantics. Returns the number of newly added
    /// samples and whether the policy asked to be polled again.
    fn poll_into(
        &self,
        scratch: &mut SampleContainer<MockSample<T>>,
        max_samples: usize,
    ) -> Result<(usize, bool)> {
        let outcome = self.sub_policy.lock().expect("poisoned").poll_samples(max_samples)?;
        let mut added = 0usize;
        let retry = matches!(outcome, PollOutcome::Retry);
        if let PollOutcome::Ready(mut values) = outcome {
            for value in values.drain(..) {
                // Replacement semantics: drop the oldest sample before pushing
                // once the container would exceed `max_samples`.
                // `sample_count()` already reflects everything pushed so far in
                // this call, so only the pending push is counted additionally.
                if scratch.sample_count() + 1 > max_samples {
                    scratch.pop_front();
                }
                scratch
                    .push_back(MockSample::new(value))
                    .expect("capacity kept in check by pop_front above");
                added += 1;
            }
            if added > 0 {
                self.wake_receiver();
            }
        }
        Ok((added, retry))
    }

    fn wake_receiver(&self) {
        if let Some(waker) = self.waker.lock().expect("poisoned").take() {
            waker.wake();
        }
    }
}

impl<T: CommData + Debug> Subscription<T, MockRuntimeImpl> for MockSubscriptionImpl<T> {
    type Subscriber = MockSubscriberImpl<T>;
    type Sample<'a>
        = MockSample<T>
    where
        Self: 'a;

    fn unsubscribe(self) -> Self::Subscriber {
        self.sub_policy.lock().expect("poisoned").unsubscribe();
        MockSubscriberImpl {
            info: self.info,
            identifier: self.identifier,
            policy: self.subscriber_policy,
        }
    }

    fn try_receive<'a>(
        &'a self,
        scratch: &'_ mut SampleContainer<Self::Sample<'a>>,
        max_samples: usize,
    ) -> Result<usize> {
        self.validate_max_samples(max_samples)?;
        Ok(self.poll_into(scratch, max_samples)?.0)
    }

    #[allow(clippy::manual_async_fn)]
    fn cancellable_receive<'a>(
        &'a self,
        scratch: SampleContainer<Self::Sample<'a>>,
        new_samples: usize,
        max_samples: usize,
        cancellation: impl Future<Output = ()> + Send + 'static,
    ) -> impl Future<Output = (SampleContainer<Self::Sample<'a>>, Result<usize>)> + 'a {
        // The opaque `impl Future` must have one concrete type, so all return
        // paths go through this single `poll_future`.
        let mut validation_error = match self.validate_bounds(new_samples, max_samples) {
            Ok(()) => None,
            Err(e) => Some(e),
        };
        let mut scratch = Some(scratch);
        let mut total = 0usize;
        // `Pin<Box<F>>` is `Unpin` for any `F`, so the cancellation future can
        // be polled in place across polls without an `Unpin` bound.
        let mut cancellation = Pin::from(Box::new(cancellation));
        poll_future(move |cx: &mut Context<'_>| {
            if let Some(e) = validation_error.take() {
                return Poll::Ready((
                    scratch.take().expect("container always returned"),
                    Err(e),
                ));
            }
            // Cancellation wins as soon as it is ready (Lola parity).
            if cancellation.as_mut().poll(cx).is_ready() {
                return Poll::Ready((
                    scratch.take().expect("container always returned"),
                    Err(Error::ReceiveError(ReceiveFailedReason::Cancelled)),
                ));
            }
            let (added, retry) = {
                let inner = scratch
                    .as_mut()
                    .expect("container stays in place until completion");
                match self.poll_into(inner, max_samples) {
                    Ok(value) => value,
                    Err(e) => {
                        return Poll::Ready((
                            scratch.take().expect("container always returned"),
                            Err(e),
                        ))
                    }
                }
            };
            total += added;
            if total >= new_samples {
                return Poll::Ready((
                    scratch.take().expect("container always returned"),
                    Ok(total),
                ));
            }
            // Park. `Retry` means "poll me again" — wake ourselves so a
            // bounded scripted retry sequence makes progress. `Idle` (and a
            // short `Ready`) park until something else wakes the receiver.
            *self.waker.lock().expect("poisoned") = Some(cx.waker().clone());
            if retry {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        })
    }

    fn to_stream<'a>(&'a mut self) -> impl Stream<Item = Result<Self::Sample<'a>>> + Unpin + 'a {
        poll_fn(move |cx: &mut Context<'_>| {
            // Drain the buffered batch one sample at a time.
            let mut buffered = self.buffered.lock().expect("poisoned");
            if let Some(value) = buffered.pop_front() {
                return Poll::Ready(Some(Ok(MockSample::new(value))));
            }
            drop(buffered);

            match self
                .sub_policy
                .lock()
                .expect("poisoned")
                .poll_samples(self.max_num_samples)
            {
                Err(e) => {
                    // Yield the error, keep the stream alive for later samples.
                    Poll::Ready(Some(Err(e)))
                }
                Ok(PollOutcome::Retry) => {
                    *self.waker.lock().expect("poisoned") = Some(cx.waker().clone());
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Ok(PollOutcome::Idle) => {
                    *self.waker.lock().expect("poisoned") = Some(cx.waker().clone());
                    Poll::Pending
                }
                Ok(PollOutcome::Ready(mut batch)) => {
                    if !batch.is_empty() {
                        let value = batch.remove(0);
                        let mut buffered = self.buffered.lock().expect("poisoned");
                        buffered.extend(batch);
                        drop(buffered);
                        self.wake_receiver();
                        return Poll::Ready(Some(Ok(MockSample::new(value))));
                    }
                    *self.waker.lock().expect("poisoned") = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }
}
