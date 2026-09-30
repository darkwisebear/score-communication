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

//! `Subscriber` + `Subscription` for the fake backend.
//!
//! `Subscription::Sample<'a>` is a GAT that mockall 0.14 cannot mock, so the
//! types in this module are hand-written. Unlike the mockall backend, every
//! receive path is a **real** operation: it pulls values out of the shared
//! [`EventQueue`](crate::registry::EventQueue) for the subscribed event, so
//! samples sent by a `Publisher` are actually observed here — no expectation
//! scripting involved.
//!
//! Delivery model: samples are retained in the queue until a subscription
//! consumes them. A subscriber established *after* samples were already sent
//! still receives the not-yet-consumed samples in send order, and each
//! subscription receives a given sample at most once (the queue is a shared
//! consume-on-read buffer, so a second subscription on the same event does
//! not re-deliver what the first already took). Replacement semantics of
//! `try_receive`/`receive` follow the contract in
//! [`score_com_concept::Subscription`]: the buffer keeps at most `max_samples`
//! entries, dropping the oldest as new samples arrive.
//!
//! [`FakeSample`] **owns** its `T` (no `Arc`): the `Runtime` GAT
//! (`type Subscriber<T: CommData + Debug>`) requires the impl to hold for any
//! `T: CommData + Debug`, and `Arc<T>: Send` would demand `T: Sync`, which
//! `CommData` does not imply.

use crate::consumer::FakeConsumerInfo;
use crate::registry::{validate_bounds, validate_max_samples};
use crate::runtime::FakeRuntimeImpl;
use core::cmp::Ordering;
use core::fmt::{self, Debug};
use core::future::Future;
use core::ops::Deref;
use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use core::task::{Context, Poll};
use futures::future::poll_fn as poll_future;
use futures::stream::{poll_fn, Stream};
use score_com_concept::{
    CommData, Error, ReceiveFailedReason, Result, Sample, SampleContainer, Subscription,
    Subscriber,
};

/// Hand-written sample for received data. It exists to satisfy the `Sample`
/// supertrait bounds; it owns the received value and compares by **reception
/// order**, so each instance carries a process-wide sequence number.
pub struct FakeSample<T: CommData + Debug> {
    order: usize,
    data: T,
}

impl<T: CommData + Debug> FakeSample<T> {
    fn new(data: T) -> Self {
        Self {
            order: SAMPLE_ORDER.fetch_add(1, AtomicOrdering::SeqCst),
            data,
        }
    }
}

impl<T: CommData + Debug> Deref for FakeSample<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T: CommData + Debug> Debug for FakeSample<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeSample").field("data", &self.data).finish()
    }
}

impl<T: CommData + Debug> PartialOrd for FakeSample<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: CommData + Debug> Ord for FakeSample<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order.cmp(&other.order)
    }
}

impl<T: CommData + Debug> PartialEq for FakeSample<T> {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }
}

impl<T: CommData + Debug> Eq for FakeSample<T> {}

impl<T: CommData + Debug> Sample<T> for FakeSample<T> {}

/// Process-wide reception order shared by all fake samples so that
/// `PartialOrd`/`Ord` reflect reception order, as the trait docs require.
static SAMPLE_ORDER: AtomicUsize = AtomicUsize::new(0);

/// `Subscriber` for one event of one instance of the fake backend.
pub struct FakeSubscriber<T: CommData + Debug> {
    info: FakeConsumerInfo,
    identifier: &'static str,
    _t: core::marker::PhantomData<T>,
}

impl<T: CommData + Debug> Debug for FakeSubscriber<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeSubscriber")
            .field("identifier", &self.identifier)
            .finish()
    }
}

impl<T: CommData + Debug> Subscriber<T, FakeRuntimeImpl> for FakeSubscriber<T> {
    type Subscription = FakeSubscription<T>;

    fn new(identifier: &'static str, instance_info: FakeConsumerInfo) -> Result<Self> {
        // There is nothing to fail on in the in-memory backend.
        Ok(Self {
            info: instance_info,
            identifier,
            _t: core::marker::PhantomData,
        })
    }

    fn subscribe(self, max_num_samples: usize) -> Result<Self::Subscription> {
        if max_num_samples == 0 {
            return Err(Error::ReceiveError(ReceiveFailedReason::InputValueOutOfBounds {
                max: 0,
                requested: 0,
            }));
        }
        Ok(FakeSubscription::<T> {
            info: self.info,
            identifier: self.identifier,
            max_num_samples,
            _t: core::marker::PhantomData,
        })
    }
}

/// Active subscription: pulls published samples from the shared event queue.
pub struct FakeSubscription<T: CommData + Debug> {
    info: FakeConsumerInfo,
    identifier: &'static str,
    max_num_samples: usize,
    _t: core::marker::PhantomData<T>,
}

impl<T: CommData + Debug> Debug for FakeSubscription<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeSubscription")
            .field("identifier", &self.identifier)
            .field("max_num_samples", &self.max_num_samples)
            .finish()
    }
}

impl<T: CommData + Debug> FakeSubscription<T> {
    /// The retained queue this subscription reads from.
    fn queue(&self) -> std::sync::Arc<crate::registry::EventQueue<T>> {
        self.info.registry().queue_for::<T>(
            self.info.interface_id(),
            &self.info.instance(),
            self.identifier,
        )
    }

    /// Apply the replacement semantics: push the drained `batch` into
    /// `scratch`, dropping the oldest sample whenever the container would
    /// exceed `max_samples`. Returns the number of newly added samples.
    fn push_batch(
        &self,
        scratch: &mut SampleContainer<FakeSample<T>>,
        max_samples: usize,
        batch: Vec<T>,
    ) -> usize {
        let mut added = 0usize;
        for value in batch {
            if scratch.sample_count() + 1 > max_samples {
                scratch.pop_front();
            }
            scratch
                .push_back(FakeSample::new(value))
                .expect("capacity kept in check by pop_front above");
            added += 1;
        }
        added
    }
}

impl<T: CommData + Debug> Subscription<T, FakeRuntimeImpl> for FakeSubscription<T> {
    type Subscriber = FakeSubscriber<T>;
    type Sample<'a>
        = FakeSample<T>
    where
        Self: 'a;

    fn unsubscribe(self) -> Self::Subscriber {
        FakeSubscriber::<T> {
            info: self.info,
            identifier: self.identifier,
            _t: core::marker::PhantomData,
        }
    }

    fn try_receive<'a>(
        &'a self,
        scratch: &'_ mut SampleContainer<Self::Sample<'a>>,
        max_samples: usize,
    ) -> Result<usize> {
        validate_max_samples(self.max_num_samples, max_samples)?;
        let batch = self.queue().drain_front(max_samples);
        Ok(self.push_batch(scratch, max_samples, batch))
    }

    #[allow(clippy::manual_async_fn)]
    fn cancellable_receive<'a>(
        &'a self,
        scratch: SampleContainer<Self::Sample<'a>>,
        new_samples: usize,
        max_samples: usize,
        cancellation: impl Future<Output = ()> + Send + 'static,
    ) -> impl Future<Output = (SampleContainer<Self::Sample<'a>>, Result<usize>)> + 'a {
        let mut validation_error =
            match validate_bounds(self.max_num_samples, new_samples, max_samples) {
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
            // Cancellation wins as soon as it is ready.
            if cancellation.as_mut().poll(cx).is_ready() {
                return Poll::Ready((
                    scratch.take().expect("container always returned"),
                    Err(Error::ReceiveError(ReceiveFailedReason::Cancelled)),
                ));
            }
            let remaining = new_samples.saturating_sub(total);
            let added = {
                let inner = scratch
                    .as_mut()
                    .expect("container stays in place until completion");
                // Drain what is available (registering the waker atomically if
                // the queue is empty so a publish can never be missed).
                let batch = self.queue().drain_or_wait(remaining, cx.waker());
                match batch {
                    Some(values) => self.push_batch(inner, max_samples, values),
                    None => 0,
                }
            };
            total += added;
            if total >= new_samples {
                return Poll::Ready((
                    scratch.take().expect("container always returned"),
                    Ok(total),
                ));
            }
            // Nothing (more) available and not enough yet: `drain_or_wait`
            // has already parked the waker for us.
            Poll::Pending
        })
    }

    fn to_stream<'a>(&'a mut self) -> impl Stream<Item = Result<Self::Sample<'a>>> + Unpin + 'a {
        poll_fn(move |cx: &mut Context<'_>| {
            match self.queue().drain_or_wait(1, cx.waker()) {
                // A value is available: own it and yield it.
                Some(mut batch) => Poll::Ready(Some(Ok(FakeSample::new(
                    batch.pop().expect("batch is non-empty"),
                )))),
                // Nothing pending: `drain_or_wait` parked the waker.
                None => Poll::Pending,
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::publisher::FakePublisher;
    use crate::provider_info::FakeProviderInfo;
    use crate::registry::FakeRegistry;
    use score_com_concept::{InstanceSpecifier, Publisher, Subscriber};

    #[derive(Debug, Clone, PartialEq)]
    #[repr(C)]
    struct Payload {
        value: i32,
    }

    unsafe impl score_com_concept::Reloc for Payload {}

    impl score_com_concept::CommData for Payload {
        const ID: &'static str = "Payload";
    }

    fn spec() -> InstanceSpecifier {
        InstanceSpecifier::new("/Test/Service1/Instance").expect("valid instance specifier")
    }

    fn info() -> FakeConsumerInfo {
        FakeConsumerInfo::new("Test", spec(), FakeRegistry::new())
    }

    #[test]
    fn subscribe_with_zero_capacity_fails() {
        let subscriber =
            FakeSubscriber::<Payload>::new("evt", info()).expect("new subscriber");
        assert!(subscriber.subscribe(0).is_err());
    }

    /// The core Plan A guarantee: a value pushed through a `Publisher` on the
    /// shared registry is observed by a `Subscription` on the same endpoint,
    /// with replacement semantics and the sample dereferencing to its value.
    #[test]
    fn publisher_to_subscription_roundtrip() {
        let registry = FakeRegistry::new();
        let spec = spec();
        let provider =
            FakeProviderInfo::new("Test", spec.clone(), registry.clone());

        // Publisher created from the provider info (as `interface!` does).
        let publisher =
            FakePublisher::<Payload>::new("evt", provider).expect("new publisher");

        // Subscriber created from consumer info sharing the registry.
        let consumer =
            FakeConsumerInfo::new("Test", spec.clone(), registry.clone());
        let subscriber =
            FakeSubscriber::<Payload>::new("evt", consumer).expect("new subscriber");
        let subscription = subscriber
            .subscribe(10)
            .expect("subscribe");

        // Send three samples.
        publisher
            .send(Payload { value: 1 })
            .expect("send 1");
        publisher
            .send(Payload { value: 2 })
            .expect("send 2");
        publisher
            .send(Payload { value: 3 })
            .expect("send 3");

        // A late subscriber still receives the retained, not-yet-consumed
        // samples in send order.
        let mut container = SampleContainer::new(10);
        let added = subscription
            .try_receive(&mut container, 10)
            .expect("try_receive");
        assert_eq!(added, 3);
        let values: Vec<i32> = container.iter::<Payload>().map(|p| p.value).collect();
        assert_eq!(values, vec![1, 2, 3]);

        // The queue is consumed on read: a second receive yields nothing.
        let mut empty = SampleContainer::new(10);
        assert_eq!(
            subscription.try_receive(&mut empty, 10).expect("again"),
            0
        );
    }

    /// Replacement semantics: the container keeps at most `max_samples`,
    /// dropping the oldest as new samples arrive.
    #[test]
    fn try_receive_applies_replacement_semantics() {
        let registry = FakeRegistry::new();
        let spec = spec();
        let publisher = FakePublisher::<Payload>::new(
            "evt",
            FakeProviderInfo::new("Test", spec.clone(), registry.clone()),
        )
        .expect("publisher");
        let consumer = FakeConsumerInfo::new("Test", spec.clone(), registry.clone());
        let subscription = FakeSubscriber::<Payload>::new("evt", consumer)
            .expect("subscriber")
            .subscribe(10)
            .expect("subscribe");

        // Capacity 2; send 1..=4, receive in two calls of max 2.
        for v in 1..=4 {
            publisher.send(Payload { value: v }).expect("send");
        }
        let mut container = SampleContainer::new(10);
        subscription.try_receive(&mut container, 2).expect("receive");
        let first: Vec<i32> = container.iter::<Payload>().map(|p| p.value).collect();
        assert_eq!(first, vec![1, 2]);
        // A subsequent receive drops the oldest and keeps the newest two.
        subscription.try_receive(&mut container, 2).expect("receive again");
        let second: Vec<i32> = container.iter::<Payload>().map(|p| p.value).collect();
        assert_eq!(second, vec![3, 4]);
    }

    /// `to_stream` yields the published samples one at a time. Both samples
    /// are pre-sent, so a single-threaded executor can drive the stream
    /// without needing a multi-threaded runtime.
    #[test]
    fn to_stream_yields_published_samples() {
        let registry = FakeRegistry::new();
        let spec = spec();
        let publisher = FakePublisher::<Payload>::new(
            "evt",
            FakeProviderInfo::new("Test", spec.clone(), registry.clone()),
        )
        .expect("publisher");
        let consumer = FakeConsumerInfo::new("Test", spec.clone(), registry.clone());
        let mut subscription = FakeSubscriber::<Payload>::new("evt", consumer)
            .expect("subscriber")
            .subscribe(10)
            .expect("subscribe");

        // Send two samples before streaming.
        publisher.send(Payload { value: 7 }).expect("send");
        publisher.send(Payload { value: 8 }).expect("send");

        let mut stream = subscription.to_stream();
        // Both samples are pre-sent, so each poll is immediately `Ready` and
        // a no-op waker suffices (no executor required). `to_stream` is
        // `Unpin`, so it can be polled directly.
        let waker = futures::task::noop_waker();
        let mut cx = core::task::Context::from_waker(&waker);
        let first = match Stream::poll_next(Pin::new(&mut stream), &mut cx) {
            core::task::Poll::Ready(item) => item.expect("stream alive").expect("sample"),
            core::task::Poll::Pending => unreachable!("samples are pre-sent"),
        };
        assert_eq!(first.value, 7);
        let second = match Stream::poll_next(Pin::new(&mut stream), &mut cx) {
            core::task::Poll::Ready(item) => item.expect("stream alive").expect("sample"),
            core::task::Poll::Pending => unreachable!("samples are pre-sent"),
        };
        assert_eq!(second.value, 8);
        drop(stream);
    }

    /// `unsubscribe` hands back a usable subscriber.
    #[test]
    fn unsubscribe_returns_subscriber() {
        let consumer = info();
        let subscriber = FakeSubscriber::<Payload>::new("evt", consumer).expect("subscriber");
        let subscription = subscriber.subscribe(5).expect("subscribe");
        let _recovered = subscription.unsubscribe();
        // The recovered subscriber can subscribe again on a fresh endpoint.
        assert!(
            FakeSubscriber::<Payload>::new("other", info())
                .expect("new subscriber")
                .subscribe(5)
                .is_ok()
        );
    }
}
