//! Named future and blocking adapter over one assigned-consumer batch waiter.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use super::{AssignedConsumerRecvError, AssignedConsumerRecvRegistration};
use crate::consumer::assigned_host::{AssignedConsumerBatch, AssignedConsumerHandle};

/// Runtime-neutral observation of one already-authorized assigned-consumer batch.
#[must_use = "dropping recv abandons only observation; background Fetch continues"]
pub struct AssignedConsumerRecv<'consumer> {
    handle: &'consumer mut AssignedConsumerHandle,
    registration: Option<AssignedConsumerRecvRegistration>,
}

impl<'consumer> AssignedConsumerRecv<'consumer> {
    pub(in crate::consumer::assigned_host) const fn new(
        handle: &'consumer mut AssignedConsumerHandle,
    ) -> Self {
        Self {
            handle,
            registration: None,
        }
    }

    /// Blocks on the same generation-fenced signal used by [`Future::poll`].
    pub fn wait(mut self) -> Result<Option<AssignedConsumerBatch>, AssignedConsumerRecvError> {
        let delivery = self.handle.port.wait_recv(&mut self.registration)?;
        Ok(delivery.map(|delivery| {
            AssignedConsumerBatch::new(delivery, Arc::clone(&self.handle.port.shared))
        }))
    }
}

impl Future for AssignedConsumerRecv<'_> {
    type Output = Result<Option<AssignedConsumerBatch>, AssignedConsumerRecvError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.handle
            .port
            .poll_recv(&mut this.registration, context)
            .map(|result| {
                result.map(|delivery| {
                    delivery.map(|delivery| {
                        AssignedConsumerBatch::new(delivery, Arc::clone(&this.handle.port.shared))
                    })
                })
            })
    }
}

impl Drop for AssignedConsumerRecv<'_> {
    fn drop(&mut self) {
        self.handle.port.cancel_recv(&mut self.registration);
    }
}

impl std::fmt::Debug for AssignedConsumerRecv<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerRecv")
            .field("registered", &self.registration.is_some())
            .finish_non_exhaustive()
    }
}
