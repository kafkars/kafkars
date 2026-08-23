//! Named future and blocking adapter over one hosted share batch waiter.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::{ShareConsumerRecvError, ShareConsumerRecvRegistration, ShareConsumerRecvWait};
use crate::consumer::{
    ShareConsumerBatch, ShareConsumerHandle, ShareConsumerTryTakeBatchErrorKind,
};

/// Runtime-neutral observation of one already-authorized share batch.
#[must_use = "dropping recv abandons only observation; background ShareFetch continues"]
pub struct ShareConsumerRecv<'consumer> {
    handle: &'consumer mut ShareConsumerHandle,
    registration: Option<ShareConsumerRecvRegistration>,
}

impl<'consumer> ShareConsumerRecv<'consumer> {
    pub(in crate::consumer) const fn new(handle: &'consumer mut ShareConsumerHandle) -> Self {
        Self {
            handle,
            registration: None,
        }
    }

    /// Blocks on the same bounded generation signal used by [`Future::poll`].
    pub fn wait(mut self) -> Result<Option<ShareConsumerBatch>, ShareConsumerRecvError> {
        loop {
            let id = self.handle.port.arm_share_recv_blocking(
                self.handle.group_id,
                self.registration,
                ShareConsumerRecvWait::Unlock,
            )?;
            self.registration = Some(id);
            match self.probe(None)? {
                Poll::Ready(result) => return Ok(result),
                Poll::Pending => self.handle.port.wait_share_recv(id)?,
            }
        }
    }

    fn probe(
        &mut self,
        context: Option<&Context<'_>>,
    ) -> Result<Poll<Option<ShareConsumerBatch>>, ShareConsumerRecvError> {
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
                ShareConsumerTryTakeBatchErrorKind::Contended => Ok(Poll::Pending),
                ShareConsumerTryTakeBatchErrorKind::Closed
                | ShareConsumerTryTakeBatchErrorKind::ConsumerUnavailable => {
                    self.finish();
                    Ok(Poll::Ready(None))
                }
                ShareConsumerTryTakeBatchErrorKind::Pending => {
                    self.rearm_change(context)?;
                    Ok(Poll::Pending)
                }
                ShareConsumerTryTakeBatchErrorKind::HostUnavailable => {
                    self.finish();
                    Err(ShareConsumerRecvError::host_unavailable())
                }
                ShareConsumerTryTakeBatchErrorKind::InternalInvariant => {
                    self.finish();
                    Err(ShareConsumerRecvError::internal_invariant())
                }
            },
        }
    }

    fn rearm_change(
        &mut self,
        context: Option<&Context<'_>>,
    ) -> Result<(), ShareConsumerRecvError> {
        let Some(current) = self.registration else {
            return Err(ShareConsumerRecvError::internal_invariant());
        };
        let id = match context {
            Some(context) => self.handle.port.rearm_share_recv_task(
                self.handle.group_id,
                current,
                ShareConsumerRecvWait::Change,
                context.waker(),
            )?,
            None => self.handle.port.rearm_share_recv_blocking(
                self.handle.group_id,
                current,
                ShareConsumerRecvWait::Change,
            )?,
        };
        self.registration = Some(id);
        Ok(())
    }

    fn finish(&mut self) {
        self.handle.port.cancel_share_recv(&mut self.registration);
    }
}

impl Future for ShareConsumerRecv<'_> {
    type Output = Result<Option<ShareConsumerBatch>, ShareConsumerRecvError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let id = match this.handle.port.arm_share_recv_task(
            this.handle.group_id,
            this.registration,
            ShareConsumerRecvWait::Unlock,
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

impl Drop for ShareConsumerRecv<'_> {
    fn drop(&mut self) {
        self.finish();
    }
}

impl std::fmt::Debug for ShareConsumerRecv<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareConsumerRecv")
            .field("registered", &self.registration.is_some())
            .finish_non_exhaustive()
    }
}

impl ShareConsumerHandle {
    /// Waits only for delivery already authorized by hosted background `ShareFetch`.
    pub fn recv(&mut self) -> ShareConsumerRecv<'_> {
        ShareConsumerRecv::new(self)
    }
}
