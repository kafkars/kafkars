//! Public linear admission and terminal observation of one share-member close.

use std::{future::Future, pin::Pin, task::Poll};

use kafka_client_core::ShareGroupHeartbeatFailure;

use super::{
    close_state::{ShareConsumerCloseCompletion, ShareConsumerCloseTerminal},
    port::ShareClosePortError,
    public_registration::ShareConsumerHandle,
    registry_close::ShareConsumerCloseAdmissionError as RegistryCloseError,
    shard::ShareConsumerShardLockError,
};

/// Stable reason explicit close did not consume the share handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareConsumerCloseAdmissionErrorKind {
    /// Engine-wide share admission has closed.
    Closed,
    /// Another owner temporarily holds the share registry.
    Contended,
    /// The exact registration is absent or already closing.
    Unavailable,
    /// Clock, completion, or host ownership is inconsistent.
    Internal,
}

/// Pre-admission rejection retaining the exact share consumer.
#[derive(Debug)]
#[must_use = "close rejection retains the exact share handle"]
pub struct ShareConsumerCloseAdmissionError {
    kind: ShareConsumerCloseAdmissionErrorKind,
    handle: ShareConsumerHandle,
}

impl ShareConsumerCloseAdmissionError {
    /// Returns the stable pre-admission rejection category.
    pub const fn kind(&self) -> ShareConsumerCloseAdmissionErrorKind {
        self.kind
    }

    /// Recovers the exact still-live handle whose close was rejected.
    pub fn into_handle(self) -> ShareConsumerHandle {
        self.handle
    }
}

impl core::fmt::Display for ShareConsumerCloseAdmissionError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "share close rejected: {:?}", self.kind)
    }
}

impl std::error::Error for ShareConsumerCloseAdmissionError {}

/// Stable terminal share-close category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareConsumerCloseErrorKind {
    /// The original close deadline elapsed.
    DeadlineElapsed,
    /// No usable share membership coordinator route remained.
    CoordinatorUnavailable,
    /// The broker lacks a compatible share-heartbeat version.
    Compatibility,
    /// Driver or host execution ended terminally.
    Execution,
    /// Kafka returned one exact nonzero error code.
    BrokerRejected,
    /// A successful response violated protocol bounds or shape.
    InvalidResponse,
    /// Completion observation violated an engine invariant.
    Internal,
}

/// Terminal failure of an accepted share close.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerCloseError {
    kind: ShareConsumerCloseErrorKind,
    broker_code: Option<i16>,
}

impl ShareConsumerCloseError {
    /// Returns the stable terminal category.
    pub const fn kind(self) -> ShareConsumerCloseErrorKind {
        self.kind
    }

    /// Returns the exact Kafka error code for broker rejection.
    pub const fn broker_code(self) -> Option<i16> {
        self.broker_code
    }
}

impl core::fmt::Display for ShareConsumerCloseError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "share close failed: {:?}", self.kind)
    }
}

impl std::error::Error for ShareConsumerCloseError {}

/// Sole runtime-neutral observer for one accepted share close.
#[derive(Debug)]
#[must_use = "dropping observation does not cancel accepted share close"]
pub struct ShareConsumerClose {
    completion: ShareConsumerCloseCompletion,
    wake_failed: bool,
}

impl ShareConsumerClose {
    /// Reports advisory wake degradation after accepted close ownership.
    pub const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    /// Blocks on the same completion cell used by [`Future::poll`].
    pub fn wait(self) -> Result<(), ShareConsumerCloseError> {
        self.completion
            .wait()
            .map_err(|_error| internal_error())
            .and_then(terminal_result)
    }
}

impl Future for ShareConsumerClose {
    type Output = Result<(), ShareConsumerCloseError>;

    fn poll(self: Pin<&mut Self>, context: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.completion).poll(context) {
            Poll::Ready(Ok(terminal)) => Poll::Ready(terminal_result(terminal)),
            Poll::Ready(Err(_error)) => Poll::Ready(Err(internal_error())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl ShareConsumerHandle {
    /// Begins one bounded graceful Leave after reserving terminal capacity.
    pub fn try_close(self) -> Result<ShareConsumerClose, ShareConsumerCloseAdmissionError> {
        match self.port.try_begin_close(self.group_id, self.close_timeout) {
            Ok(admission) => {
                let wake_failed = admission.wake_failed();
                Ok(ShareConsumerClose {
                    completion: admission.completion,
                    wake_failed,
                })
            }
            Err(error) => Err(ShareConsumerCloseAdmissionError {
                kind: close_admission_kind(error),
                handle: self,
            }),
        }
    }
}

pub(super) const fn close_admission_kind(
    error: ShareClosePortError,
) -> ShareConsumerCloseAdmissionErrorKind {
    match error {
        ShareClosePortError::Closed => ShareConsumerCloseAdmissionErrorKind::Closed,
        ShareClosePortError::Lock(ShareConsumerShardLockError::Contended) => {
            ShareConsumerCloseAdmissionErrorKind::Contended
        }
        ShareClosePortError::Registry(
            RegistryCloseError::UnknownConsumer | RegistryCloseError::AlreadyClosing,
        ) => ShareConsumerCloseAdmissionErrorKind::Unavailable,
        ShareClosePortError::Clock(_)
        | ShareClosePortError::Lock(ShareConsumerShardLockError::Poisoned)
        | ShareClosePortError::Registry(RegistryCloseError::Completion(_)) => {
            ShareConsumerCloseAdmissionErrorKind::Internal
        }
    }
}

const fn terminal_result(
    terminal: ShareConsumerCloseTerminal,
) -> Result<(), ShareConsumerCloseError> {
    match terminal {
        ShareConsumerCloseTerminal::Succeeded => Ok(()),
        ShareConsumerCloseTerminal::Failed(failure) => Err(terminal_error(failure)),
    }
}

pub(super) const fn terminal_error(failure: ShareGroupHeartbeatFailure) -> ShareConsumerCloseError {
    let (kind, broker_code) = match failure {
        ShareGroupHeartbeatFailure::DeadlineElapsed => {
            (ShareConsumerCloseErrorKind::DeadlineElapsed, None)
        }
        ShareGroupHeartbeatFailure::CoordinatorUnavailable => {
            (ShareConsumerCloseErrorKind::CoordinatorUnavailable, None)
        }
        ShareGroupHeartbeatFailure::Compatibility => {
            (ShareConsumerCloseErrorKind::Compatibility, None)
        }
        ShareGroupHeartbeatFailure::Execution => (ShareConsumerCloseErrorKind::Execution, None),
        ShareGroupHeartbeatFailure::Broker(code) => {
            (ShareConsumerCloseErrorKind::BrokerRejected, Some(code))
        }
        ShareGroupHeartbeatFailure::InvalidResponse => {
            (ShareConsumerCloseErrorKind::InvalidResponse, None)
        }
    };
    ShareConsumerCloseError { kind, broker_code }
}

const fn internal_error() -> ShareConsumerCloseError {
    ShareConsumerCloseError {
        kind: ShareConsumerCloseErrorKind::Internal,
        broker_code: None,
    }
}
