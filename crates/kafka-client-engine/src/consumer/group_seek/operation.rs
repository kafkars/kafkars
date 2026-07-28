//! Runtime-neutral observation of one accepted group-consumer seek.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::consumer::{
    GroupConsumerHandle,
    group::{GroupConsumerSeekObservation, GroupConsumerSeekObservationError},
    group_recv::{GroupConsumerRecvRegistration, GroupConsumerRecvWait},
};

use super::{
    GroupConsumerSeekCompletion, GroupConsumerSeekCompletionObservation, GroupConsumerSeekTerminal,
    GroupConsumerSeekTerminalFailure, GroupConsumerSeekTerminalFailureKind,
    error::{GroupConsumerSeekError, host_unavailable_error, internal_error, terminal_error},
};

/// Named terminal observer for one accepted seek.
#[must_use = "dropping abandons observation without cancelling accepted seek"]
pub struct GroupConsumerSeek<'handle> {
    handle: &'handle mut GroupConsumerHandle,
    completion: Arc<GroupConsumerSeekCompletion>,
    registration: Option<GroupConsumerRecvRegistration>,
    wake_failed: bool,
}

impl<'handle> GroupConsumerSeek<'handle> {
    pub(in crate::consumer) fn new(
        handle: &'handle mut GroupConsumerHandle,
        completion: Arc<GroupConsumerSeekCompletion>,
        registration: GroupConsumerRecvRegistration,
        wake_failed: bool,
    ) -> Self {
        Self {
            handle,
            completion,
            registration: Some(registration),
            wake_failed,
        }
    }

    /// Reports advisory reactor-wake degradation after accepted mutation.
    pub const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    /// Blocks on the same bounded off-reactor signal used by [`Future::poll`].
    pub fn wait(mut self) -> Result<(), GroupConsumerSeekError> {
        loop {
            let registration = self.registration.ok_or_else(internal_error)?;
            match self.probe()? {
                Poll::Ready(()) => return Ok(()),
                Poll::Pending => {
                    self.handle
                        .port
                        .wait_group_recv(registration)
                        .map_err(|_error| internal_error())?;
                    self.registration = Some(
                        self.handle
                            .port
                            .arm_group_recv_blocking(
                                self.handle.group_id,
                                self.registration,
                                GroupConsumerRecvWait::Unlock,
                            )
                            .map_err(|_error| internal_error())?,
                    );
                }
            }
        }
    }

    fn probe(&mut self) -> Result<Poll<()>, GroupConsumerSeekError> {
        if let Some(terminal) = self.probe_completion() {
            return terminal;
        }
        match self
            .handle
            .port
            .observe_seek(self.handle.group_id, &self.completion)
        {
            Ok(GroupConsumerSeekObservation::Pending) => Ok(Poll::Pending),
            Ok(GroupConsumerSeekObservation::HostUnavailable) => {
                let _published = self.completion.publish(GroupConsumerSeekTerminal::Failed(
                    GroupConsumerSeekTerminalFailure {
                        kind: GroupConsumerSeekTerminalFailureKind::HostUnavailable,
                        broker_code: None,
                    },
                ));
                self.probe_completion().unwrap_or_else(|| {
                    self.finish();
                    Err(host_unavailable_error())
                })
            }
            Ok(GroupConsumerSeekObservation::TerminalMissing) => {
                self.probe_completion().unwrap_or_else(|| {
                    self.finish();
                    Err(internal_error())
                })
            }
            Err(_error @ GroupConsumerSeekObservationError::Lock(_)) => Err(internal_error()),
        }
    }

    fn probe_completion(&mut self) -> Option<Result<Poll<()>, GroupConsumerSeekError>> {
        match self.completion.observe() {
            GroupConsumerSeekCompletionObservation::Terminal(
                GroupConsumerSeekTerminal::Succeeded,
            ) => {
                self.finish();
                Some(Ok(Poll::Ready(())))
            }
            GroupConsumerSeekCompletionObservation::Terminal(
                GroupConsumerSeekTerminal::Failed(failure),
            ) => {
                self.finish();
                Some(Err(terminal_error(failure)))
            }
            GroupConsumerSeekCompletionObservation::Corrupt => {
                self.finish();
                Some(Err(internal_error()))
            }
            GroupConsumerSeekCompletionObservation::Pending => None,
        }
    }

    fn finish(&mut self) {
        self.handle.port.cancel_group_recv(&mut self.registration);
    }
}

impl Future for GroupConsumerSeek<'_> {
    type Output = Result<(), GroupConsumerSeekError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let registration = match this.handle.port.arm_group_recv_task(
            this.handle.group_id,
            this.registration,
            GroupConsumerRecvWait::Unlock,
            context.waker(),
        ) {
            Ok(registration) => registration,
            Err(_error) => return Poll::Ready(Err(internal_error())),
        };
        this.registration = Some(registration);
        match this.probe() {
            Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
            Ok(Poll::Pending) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl Drop for GroupConsumerSeek<'_> {
    fn drop(&mut self) {
        self.finish();
    }
}

impl core::fmt::Debug for GroupConsumerSeek<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerSeek")
            .field("wake_failed", &self.wake_failed)
            .finish_non_exhaustive()
    }
}
