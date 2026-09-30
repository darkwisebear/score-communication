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

//! `Publisher` for the fake backend.
//!
//! `Publisher::SampleMaybeUninit<'a>` is a GAT that mockall 0.14 cannot mock,
//! so the sample buffer types here are hand-written thin structs. Unlike the
//! mockall backend, `send` is a **real** operation: it appends the value to
//! the shared [`EventQueue`](crate::registry::EventQueue) for
//! `(interface, instance, event)`, so a `Subscription` on the same endpoint
//! actually receives it.

use crate::provider_info::FakeProviderInfo;
use crate::runtime::FakeRuntimeImpl;
use core::fmt::{self, Debug};
use core::mem::MaybeUninit;
use core::ops::{Deref, DerefMut};
use score_com_concept::{CommData, Publisher, Result, SampleMaybeUninit, SampleMut};
use std::sync::Arc;

/// Shared send-side of one `(interface, instance, event)` endpoint. The
/// publisher and every sample it allocates hold the same handle so that
/// `send` knows which queue to deliver into. The queue already carries `T`'s
/// type, so no `PhantomData`/`T: Clone` is needed to clone the handle.
pub(crate) struct PublisherEndpoint<T: CommData + Debug> {
    pub(crate) queue: Arc<crate::registry::EventQueue<T>>,
}

impl<T: CommData + Debug> Clone for PublisherEndpoint<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

impl<T: CommData + Debug> Debug for PublisherEndpoint<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublisherEndpoint").finish_non_exhaustive()
    }
}

/// Uninitialized sample buffer handed out by [`FakePublisher::allocate`].
///
/// It carries no behavior of its own; writing a value renders it initialized
/// into a [`FakeSampleMut`].
pub struct FakeSampleMaybeUninit<'a, T: CommData + Debug> {
    data: MaybeUninit<T>,
    endpoint: PublisherEndpoint<T>,
    _lifetime: core::marker::PhantomData<&'a ()>,
}

impl<'a, T: CommData + Debug> AsMut<MaybeUninit<T>> for FakeSampleMaybeUninit<'a, T> {
    fn as_mut(&mut self) -> &mut MaybeUninit<T> {
        &mut self.data
    }
}

impl<'a, T: CommData + Debug> Debug for FakeSampleMaybeUninit<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeSampleMaybeUninit").finish_non_exhaustive()
    }
}

impl<'a, T: CommData + Debug> SampleMaybeUninit<T> for FakeSampleMaybeUninit<'a, T> {
    type SampleMut = FakeSampleMut<'a, T>;

    fn write(self, value: T) -> Self::SampleMut {
        FakeSampleMut {
            data: value,
            endpoint: self.endpoint,
            _lifetime: core::marker::PhantomData,
        }
    }

    unsafe fn assume_init(self) -> Self::SampleMut {
        FakeSampleMut {
            data: unsafe { self.data.assume_init() },
            endpoint: self.endpoint,
            _lifetime: core::marker::PhantomData,
        }
    }
}

/// Initialized sample buffer; [`SampleMut::send`] delivers the value to the
/// shared queue.
pub struct FakeSampleMut<'a, T: CommData + Debug> {
    data: T,
    endpoint: PublisherEndpoint<T>,
    _lifetime: core::marker::PhantomData<&'a ()>,
}

impl<'a, T: CommData + Debug> Deref for FakeSampleMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a, T: CommData + Debug> DerefMut for FakeSampleMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<'a, T: CommData + Debug> Debug for FakeSampleMut<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeSampleMut").field("data", &self.data).finish()
    }
}

impl<'a, T: CommData + Debug> SampleMut<T> for FakeSampleMut<'a, T> {
    fn send(self) -> Result<()> {
        self.endpoint.queue.push(self.data);
        Ok(())
    }
}

/// `Publisher` for one event of one instance of the fake backend.
pub struct FakePublisher<T: CommData + Debug> {
    endpoint: PublisherEndpoint<T>,
}

impl<T: CommData + Debug> Clone for FakePublisher<T> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
        }
    }
}

impl<T: CommData + Debug> Debug for FakePublisher<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakePublisher").finish_non_exhaustive()
    }
}

impl<T: CommData + Debug> Publisher<T, FakeRuntimeImpl> for FakePublisher<T> {
    type SampleMaybeUninit<'a>
        = FakeSampleMaybeUninit<'a, T>
    where
        Self: 'a;

    fn allocate(&self) -> Result<Self::SampleMaybeUninit<'_>> {
        Ok(FakeSampleMaybeUninit {
            data: MaybeUninit::uninit(),
            endpoint: self.endpoint.clone(),
            _lifetime: core::marker::PhantomData,
        })
    }

    fn send(&self, value: T) -> Result<()> {
        self.endpoint.queue.push(value);
        Ok(())
    }

    fn new(identifier: &str, instance_info: FakeProviderInfo) -> Result<Self> {
        // The endpoint is created lazily; it always exists for a given
        // (interface, instance, event), so there is nothing to fail on.
        let endpoint = PublisherEndpoint::from_provider_info(identifier, instance_info);
        Ok(Self { endpoint })
    }
}

impl<T: CommData + Debug> PublisherEndpoint<T> {
    pub(crate) fn from_provider_info(
        identifier: &str,
        instance_info: FakeProviderInfo,
    ) -> Self {
        let queue = instance_info.registry().queue_for::<T>(
            instance_info.interface_id(),
            &instance_info.instance(),
            identifier,
        );
        Self { queue }
    }
}
