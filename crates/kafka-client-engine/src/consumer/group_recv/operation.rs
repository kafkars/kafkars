//! Named future and blocking adapter over one hosted classic-group batch waiter.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::{GroupConsumerRecvError, GroupConsumerRecvRegistration, GroupConsumerRecvWait};
use crate::consumer::{
    GroupConsumerBatch, GroupConsumerHandle, GroupConsumerTryTakeBatchErrorKind,
};

/// Runtime-neutral observation of one already-authorized classic-group batch.
#[must_use = "dropping recv abandons only observation; background Fetch continues"]
pub struct GroupConsumerRecv<'consumer> {
    handle: &'consumer mut GroupConsumerHandle,
    registration: Option<GroupConsumerRecvRegistration>,
}

impl<'consumer> GroupConsumerRecv<'consumer> {
    pub(in crate::consumer) const fn new(handle: &'consumer mut GroupConsumerHandle) -> Self {
        Self {
            handle,
            registration: None,
        }
    }

    /// Blocks on the same bounded generation signal used by [`Future::poll`].
    pub fn wait(mut self) -> Result<Option<GroupConsumerBatch>, GroupConsumerRecvError> {
        loop {
            let id = self.handle.port.arm_group_recv_blocking(
                self.handle.group_id,
                self.registration,
                GroupConsumerRecvWait::Unlock,
            )?;
            self.registration = Some(id);
            match self.probe(None)? {
                Poll::Ready(result) => return Ok(result),
                Poll::Pending => self.handle.port.wait_group_recv(id)?,
            }
        }
    }

    fn probe(
        &mut self,
        context: Option<&Context<'_>>,
    ) -> Result<Poll<Option<GroupConsumerBatch>>, GroupConsumerRecvError> {
        match self.handle.try_take_batch() {
            Ok(Some(batch)) => {
                self.finish();
                Ok(Poll::Ready(Some(batch)))
            }
            Ok(None) => {
                self.rearm_change(context)?;
                Ok(Poll::Pending)
            }
            Err(error) => match error.kind() {
                GroupConsumerTryTakeBatchErrorKind::Contended => Ok(Poll::Pending),
                GroupConsumerTryTakeBatchErrorKind::Closed
                | GroupConsumerTryTakeBatchErrorKind::GroupUnavailable => {
                    self.finish();
                    Ok(Poll::Ready(None))
                }
                GroupConsumerTryTakeBatchErrorKind::Pending
                | GroupConsumerTryTakeBatchErrorKind::ProcessingExpired => {
                    self.rearm_change(context)?;
                    Ok(Poll::Pending)
                }
                GroupConsumerTryTakeBatchErrorKind::HostUnavailable => {
                    self.finish();
                    Err(GroupConsumerRecvError::host_unavailable())
                }
                GroupConsumerTryTakeBatchErrorKind::Position(kind) => {
                    self.finish();
                    Err(GroupConsumerRecvError::position(kind))
                }
                GroupConsumerTryTakeBatchErrorKind::InternalInvariant => {
                    self.finish();
                    Err(GroupConsumerRecvError::internal_invariant())
                }
            },
        }
    }

    fn rearm_change(
        &mut self,
        context: Option<&Context<'_>>,
    ) -> Result<(), GroupConsumerRecvError> {
        let Some(current) = self.registration else {
            return Err(GroupConsumerRecvError::internal_invariant());
        };
        let id = match context {
            Some(context) => self.handle.port.rearm_group_recv_task(
                self.handle.group_id,
                current,
                GroupConsumerRecvWait::Change,
                context.waker(),
            )?,
            None => self.handle.port.rearm_group_recv_blocking(
                self.handle.group_id,
                current,
                GroupConsumerRecvWait::Change,
            )?,
        };
        self.registration = Some(id);
        Ok(())
    }

    fn finish(&mut self) {
        self.handle.port.cancel_group_recv(&mut self.registration);
    }
}

impl Future for GroupConsumerRecv<'_> {
    type Output = Result<Option<GroupConsumerBatch>, GroupConsumerRecvError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let id = match this.handle.port.arm_group_recv_task(
            this.handle.group_id,
            this.registration,
            GroupConsumerRecvWait::Unlock,
            context.waker(),
        ) {
            Ok(id) => id,
            Err(error) => return Poll::Ready(Err(error)),
        };
        this.registration = Some(id);
        match this.probe(Some(context)) {
            Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl Drop for GroupConsumerRecv<'_> {
    fn drop(&mut self) {
        self.finish();
    }
}

impl std::fmt::Debug for GroupConsumerRecv<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerRecv")
            .field("registered", &self.registration.is_some())
            .finish_non_exhaustive()
    }
}

impl GroupConsumerHandle {
    /// Waits only for delivery already authorized by hosted background Fetch.
    pub fn recv(&mut self) -> GroupConsumerRecv<'_> {
        GroupConsumerRecv::new(self)
    }
}
