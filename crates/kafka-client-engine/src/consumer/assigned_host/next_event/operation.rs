//! Named future and blocking adapter over one retained failure-event waiter.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::{AssignedConsumerEventRegistration, AssignedConsumerNextEventError};
use crate::consumer::assigned_host::{AssignedConsumerEvent, AssignedConsumerHandle};

/// Runtime-neutral observation of one retained assigned-consumer failure event.
#[must_use = "dropping next_event abandons only observation; retained events stay queued"]
pub struct AssignedConsumerNextEvent<'consumer> {
    handle: &'consumer mut AssignedConsumerHandle,
    registration: Option<AssignedConsumerEventRegistration>,
}

impl<'consumer> AssignedConsumerNextEvent<'consumer> {
    pub(in crate::consumer::assigned_host) const fn new(
        handle: &'consumer mut AssignedConsumerHandle,
    ) -> Self {
        Self {
            handle,
            registration: None,
        }
    }

    /// Blocks on the same generation-fenced signal used by [`Future::poll`].
    pub fn wait(mut self) -> Result<Option<AssignedConsumerEvent>, AssignedConsumerNextEventError> {
        self.handle.port.wait_next_event(&mut self.registration)
    }
}

impl Future for AssignedConsumerNextEvent<'_> {
    type Output = Result<Option<AssignedConsumerEvent>, AssignedConsumerNextEventError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.handle
            .port
            .poll_next_event(&mut this.registration, context)
    }
}

impl Drop for AssignedConsumerNextEvent<'_> {
    fn drop(&mut self) {
        self.handle.port.cancel_next_event(&mut self.registration);
    }
}

impl std::fmt::Debug for AssignedConsumerNextEvent<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerNextEvent")
            .field("registered", &self.registration.is_some())
            .finish_non_exhaustive()
    }
}
