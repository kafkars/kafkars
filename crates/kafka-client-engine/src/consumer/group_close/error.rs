//! Stable terminal error vocabulary for accepted explicit close.

use super::super::group::{
    GroupConsumerCloseObservationError, GroupConsumerCloseTerminalFailure,
    GroupConsumerCloseTerminalFailureKind, GroupConsumerShardLockError,
};

/// Stable terminal observation failure for one accepted close.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerCloseError {
    kind: GroupConsumerCloseErrorKind,
    broker_code: Option<i16>,
}

impl GroupConsumerCloseError {
    /// Returns the stable terminal observation failure category.
    pub const fn kind(&self) -> GroupConsumerCloseErrorKind {
        self.kind
    }

    /// Returns Kafka's exact signed `LeaveGroup` error code when supplied.
    pub const fn broker_code(&self) -> Option<i16> {
        self.broker_code
    }
}

/// Stable category for accepted close terminal observation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerCloseErrorKind {
    /// The original close deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected bounded request ownership.
    DriverRejected,
    /// Authentication failed before the broker accepted `LeaveGroup`.
    Authentication,
    /// Transport ownership ended without a valid broker terminal.
    Transport,
    /// Kafka returned a nonzero signed `LeaveGroup` error code.
    BrokerRejected,
    /// The broker and client share no compatible classic-member version.
    Compatibility,
    /// The correlated response was structurally or semantically invalid.
    InvalidResponse,
    /// The response exceeded a configured decode bound.
    ResponseTooLarge,
    /// Hosted group execution retained a terminal fault.
    HostUnavailable,
    /// Notification or registry ownership violated an invariant.
    InternalInvariant,
}

impl core::fmt::Display for GroupConsumerCloseError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "classic-group consumer close observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerCloseError {}

pub(super) const fn observation_error(
    error: GroupConsumerCloseObservationError,
) -> GroupConsumerCloseError {
    match error {
        GroupConsumerCloseObservationError::Lock(GroupConsumerShardLockError::Contended) => {
            internal_error()
        }
        GroupConsumerCloseObservationError::Lock(GroupConsumerShardLockError::Poisoned) => {
            host_unavailable_error()
        }
    }
}

pub(super) const fn internal_error() -> GroupConsumerCloseError {
    GroupConsumerCloseError {
        kind: GroupConsumerCloseErrorKind::InternalInvariant,
        broker_code: None,
    }
}

pub(super) const fn host_unavailable_error() -> GroupConsumerCloseError {
    GroupConsumerCloseError {
        kind: GroupConsumerCloseErrorKind::HostUnavailable,
        broker_code: None,
    }
}

pub(super) const fn terminal_error(
    failure: GroupConsumerCloseTerminalFailure,
) -> GroupConsumerCloseError {
    let kind = match failure.kind {
        GroupConsumerCloseTerminalFailureKind::DeadlineElapsed => {
            GroupConsumerCloseErrorKind::DeadlineElapsed
        }
        GroupConsumerCloseTerminalFailureKind::DriverRejected => {
            GroupConsumerCloseErrorKind::DriverRejected
        }
        GroupConsumerCloseTerminalFailureKind::Authentication => {
            GroupConsumerCloseErrorKind::Authentication
        }
        GroupConsumerCloseTerminalFailureKind::Transport
        | GroupConsumerCloseTerminalFailureKind::DriverShutdown => {
            GroupConsumerCloseErrorKind::Transport
        }
        GroupConsumerCloseTerminalFailureKind::BrokerRejected => {
            GroupConsumerCloseErrorKind::BrokerRejected
        }
        GroupConsumerCloseTerminalFailureKind::Compatibility => {
            GroupConsumerCloseErrorKind::Compatibility
        }
        GroupConsumerCloseTerminalFailureKind::InvalidResponse => {
            GroupConsumerCloseErrorKind::InvalidResponse
        }
        GroupConsumerCloseTerminalFailureKind::ResponseTooLarge => {
            GroupConsumerCloseErrorKind::ResponseTooLarge
        }
    };
    GroupConsumerCloseError {
        kind,
        broker_code: failure.broker_code,
    }
}
