//! Named runtime-neutral observation of one classic-group transition.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    KafkaError,
    bridge::consumer_facade::group_consumer_next_event::GroupConsumerNextEvent as BridgeNextEvent,
};

use super::{Consumer, ConsumerEvent};

impl Consumer {
    /// Takes one retained assignment transition when immediately available.
    ///
    /// This call does not wait, start group work, request a reactor turn, or
    /// reopen a terminated event stream. `Ok(None)` means no event is ready or
    /// the stream has terminated.
    pub fn try_take_event(&mut self) -> Result<Option<ConsumerEvent>, KafkaError> {
        self.engine.try_take_event()
    }

    /// Waits for one retained assignment transition without protocol work.
    ///
    /// Dropping the operation abandons only this observation. Record delivery
    /// remains an independent path.
    pub fn next_event(&mut self) -> NextConsumerEvent<'_> {
        NextConsumerEvent::from_bridge(self.engine.next_event())
    }
}

/// Waits for one retained classic-group assignment transition.
#[derive(Debug)]
#[must_use = "dropping next_event abandons only this observation"]
pub struct NextConsumerEvent<'consumer> {
    inner: BridgeNextEvent<'consumer>,
}

impl<'consumer> NextConsumerEvent<'consumer> {
    pub(crate) const fn from_bridge(inner: BridgeNextEvent<'consumer>) -> Self {
        Self { inner }
    }

    /// Blocks on the same bounded generation signal used by [`Future::poll`].
    pub fn wait(self) -> Result<Option<ConsumerEvent>, KafkaError> {
        self.inner.wait()
    }
}

impl Future for NextConsumerEvent<'_> {
    type Output = Result<Option<ConsumerEvent>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
