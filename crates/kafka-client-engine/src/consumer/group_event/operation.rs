//! Runtime-neutral wait for one retained classic-group transition.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::{GroupConsumerEvent, GroupConsumerNextEventError};
use crate::consumer::{
    GroupConsumerHandle,
    group_recv::{GroupConsumerRecvRegistration, GroupConsumerRecvWait},
};

/// Observation of one already-retained classic-group assignment transition.
#[must_use = "dropping next_event abandons only this observation"]
pub struct GroupConsumerNextEvent<'consumer> {
    handle: &'consumer mut GroupConsumerHandle,
    registration: Option<GroupConsumerRecvRegistration>,
}

impl<'consumer> GroupConsumerNextEvent<'consumer> {
    pub(in crate::consumer) const fn new(handle: &'consumer mut GroupConsumerHandle) -> Self {
        Self {
            handle,
            registration: None,
        }
    }

    /// Blocks on the same bounded generation signal used by [`Future::poll`].
    pub fn wait(mut self) -> Result<Option<GroupConsumerEvent>, GroupConsumerNextEventError> {
        loop {
            let id = self
                .handle
                .port
                .arm_group_recv_blocking(
                    self.handle.group_id,
                    self.registration,
                    GroupConsumerRecvWait::Unlock,
                )
                .map_err(|_error| GroupConsumerNextEventError::internal_invariant())?;
            self.registration = Some(id);
            match self.probe(None)? {
                Poll::Ready(result) => return Ok(result),
                Poll::Pending => self
                    .handle
                    .port
                    .wait_group_recv(id)
                    .map_err(|_error| GroupConsumerNextEventError::internal_invariant())?,
            }
        }
    }

    fn probe(
        &mut self,
        context: Option<&Context<'_>>,
    ) -> Result<Poll<Option<GroupConsumerEvent>>, GroupConsumerNextEventError> {
        match self.handle.port.try_take_event(self.handle.group_id) {
            Ok(Some(event)) => {
                self.finish();
                Ok(Poll::Ready(Some(event)))
            }
            Ok(None) => {
                self.rearm_change(context)?;
                Ok(Poll::Pending)
            }
            Err(error) if error.is_terminal() => {
                self.finish();
                Ok(Poll::Ready(None))
            }
            Err(error) if error.is_contended() => Ok(Poll::Pending),
            Err(error) if error.is_host_unavailable() => {
                self.finish();
                Err(GroupConsumerNextEventError::host_unavailable())
            }
            Err(_error) => {
                self.finish();
                Err(GroupConsumerNextEventError::internal_invariant())
            }
        }
    }

    fn rearm_change(
        &mut self,
        context: Option<&Context<'_>>,
    ) -> Result<(), GroupConsumerNextEventError> {
        let Some(current) = self.registration else {
            return Err(GroupConsumerNextEventError::internal_invariant());
        };
        let result = match context {
            Some(context) => self.handle.port.rearm_group_recv_task(
                self.handle.group_id,
                current,
                GroupConsumerRecvWait::Change,
                context.waker(),
            ),
            None => self.handle.port.rearm_group_recv_blocking(
                self.handle.group_id,
                current,
                GroupConsumerRecvWait::Change,
            ),
        };
        self.registration =
            Some(result.map_err(|_error| GroupConsumerNextEventError::internal_invariant())?);
        Ok(())
    }

    fn finish(&mut self) {
        self.handle.port.cancel_group_recv(&mut self.registration);
    }
}

impl Future for GroupConsumerNextEvent<'_> {
    type Output = Result<Option<GroupConsumerEvent>, GroupConsumerNextEventError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let id = match this.handle.port.arm_group_recv_task(
            this.handle.group_id,
            this.registration,
            GroupConsumerRecvWait::Unlock,
            context.waker(),
        ) {
            Ok(id) => id,
            Err(_error) => {
                return Poll::Ready(Err(GroupConsumerNextEventError::internal_invariant()));
            }
        };
        this.registration = Some(id);
        match this.probe(Some(context)) {
            Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl Drop for GroupConsumerNextEvent<'_> {
    fn drop(&mut self) {
        self.finish();
    }
}

impl std::fmt::Debug for GroupConsumerNextEvent<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerNextEvent")
            .field("registered", &self.registration.is_some())
            .finish_non_exhaustive()
    }
}

impl GroupConsumerHandle {
    /// Waits for the latest retained classic-group assignment transition.
    pub fn next_event(&mut self) -> GroupConsumerNextEvent<'_> {
        GroupConsumerNextEvent::new(self)
    }
}
