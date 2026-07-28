//! Runtime-neutral observation of one accepted group-consumer close.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use kafka_client_core::GroupId;

use super::{
    super::{
        GroupConsumerPort,
        group::{
            GroupConsumerCloseCompletion, GroupConsumerCloseCompletionObservation,
            GroupConsumerCloseObservation, GroupConsumerCloseTerminal,
        },
        group_recv::{GroupConsumerRecvRegistration, GroupConsumerRecvWait},
    },
    GroupConsumerCloseError,
    error::{host_unavailable_error, internal_error, observation_error, terminal_error},
};

/// Named terminal observer for one accepted explicit group-consumer close.
#[must_use = "dropping abandons observation without cancelling accepted close"]
pub struct GroupConsumerClose {
    group_id: GroupId,
    port: GroupConsumerPort,
    completion: Arc<GroupConsumerCloseCompletion>,
    registration: Option<GroupConsumerRecvRegistration>,
    wake_failed: bool,
    _lifetime: Arc<dyn Send + Sync>,
}

impl GroupConsumerClose {
    pub(in crate::consumer) fn new(
        group_id: GroupId,
        port: GroupConsumerPort,
        completion: Arc<GroupConsumerCloseCompletion>,
        registration: GroupConsumerRecvRegistration,
        wake_failed: bool,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            group_id,
            port,
            completion,
            registration: Some(registration),
            wake_failed,
            _lifetime: lifetime,
        }
    }

    /// Reports advisory reactor-wake degradation after accepted admission.
    pub const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    /// Blocks on the same bounded off-reactor signal used by [`Future::poll`].
    pub fn wait(mut self) -> Result<(), GroupConsumerCloseError> {
        loop {
            let registration = self.registration.ok_or_else(internal_error)?;
            match self.probe()? {
                Poll::Ready(()) => return Ok(()),
                Poll::Pending => {
                    self.port
                        .wait_group_recv(registration)
                        .map_err(|_error| internal_error())?;
                    let registration = self
                        .port
                        .arm_group_recv_blocking(
                            self.group_id,
                            self.registration,
                            GroupConsumerRecvWait::Unlock,
                        )
                        .map_err(|_error| internal_error())?;
                    self.registration = Some(registration);
                }
            }
        }
    }

    fn probe(&mut self) -> Result<Poll<()>, GroupConsumerCloseError> {
        match self.completion.observe() {
            GroupConsumerCloseCompletionObservation::Terminal(
                GroupConsumerCloseTerminal::Succeeded,
            ) => {
                self.finish();
                return Ok(Poll::Ready(()));
            }
            GroupConsumerCloseCompletionObservation::Terminal(
                GroupConsumerCloseTerminal::Failed(failure),
            ) => {
                self.finish();
                return Err(terminal_error(failure));
            }
            GroupConsumerCloseCompletionObservation::Corrupt => {
                self.finish();
                return Err(internal_error());
            }
            GroupConsumerCloseCompletionObservation::Pending => {}
        }
        match self.port.observe_close(self.group_id) {
            Ok(
                GroupConsumerCloseObservation::Complete
                | GroupConsumerCloseObservation::NotAccepted,
            ) => {
                self.finish();
                Err(internal_error())
            }
            Ok(GroupConsumerCloseObservation::Pending) => Ok(Poll::Pending),
            Ok(GroupConsumerCloseObservation::Faulted) => {
                self.finish();
                Err(host_unavailable_error())
            }
            Err(error) => Err(observation_error(error)),
        }
    }

    fn finish(&mut self) {
        self.port.cancel_group_recv(&mut self.registration);
    }
}

impl Future for GroupConsumerClose {
    type Output = Result<(), GroupConsumerCloseError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let registration = match this.port.arm_group_recv_task(
            this.group_id,
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

impl Drop for GroupConsumerClose {
    fn drop(&mut self) {
        self.finish();
    }
}

impl core::fmt::Debug for GroupConsumerClose {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerClose")
            .field("group_id", &self.group_id)
            .field("wake_failed", &self.wake_failed)
            .finish_non_exhaustive()
    }
}
