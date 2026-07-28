//! Stable terminal errors and exhaustive private seek-failure translation.

use super::{GroupConsumerSeekTerminalFailure, GroupConsumerSeekTerminalFailureKind};

/// Stable terminal failure for one accepted seek.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerSeekError {
    kind: GroupConsumerSeekErrorKind,
    broker_code: Option<i16>,
}

impl GroupConsumerSeekError {
    /// Returns the stable terminal failure category.
    pub const fn kind(&self) -> GroupConsumerSeekErrorKind {
        self.kind
    }

    /// Returns Kafka's exact signed error code when the broker rejected resolution.
    pub const fn broker_code(&self) -> Option<i16> {
        self.broker_code
    }
}

/// Stable terminal failure category for one accepted seek.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerSeekErrorKind {
    /// The absolute public seek deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the prepared position request.
    DriverRejected,
    /// Transport failed while resolving the requested position.
    Transport,
    /// The broker rejected position resolution.
    BrokerRejected,
    /// No compatible request version could express the operation.
    Compatibility,
    /// The broker response did not match the submitted request.
    InvalidResponse,
    /// The bounded response budget was exceeded.
    ResponseTooLarge,
    /// The assignment changed before the seek could finish.
    AssignmentLost,
    /// The synchronized engine host stopped before completion.
    HostUnavailable,
    /// Internal ownership was inconsistent.
    InternalInvariant,
}

impl core::fmt::Display for GroupConsumerSeekError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "classic-group seek observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerSeekError {}

pub(super) const fn internal_error() -> GroupConsumerSeekError {
    GroupConsumerSeekError {
        kind: GroupConsumerSeekErrorKind::InternalInvariant,
        broker_code: None,
    }
}

pub(super) const fn host_unavailable_error() -> GroupConsumerSeekError {
    GroupConsumerSeekError {
        kind: GroupConsumerSeekErrorKind::HostUnavailable,
        broker_code: None,
    }
}

pub(super) const fn terminal_error(
    failure: GroupConsumerSeekTerminalFailure,
) -> GroupConsumerSeekError {
    let kind = match failure.kind {
        GroupConsumerSeekTerminalFailureKind::DeadlineElapsed => {
            GroupConsumerSeekErrorKind::DeadlineElapsed
        }
        GroupConsumerSeekTerminalFailureKind::DriverRejected => {
            GroupConsumerSeekErrorKind::DriverRejected
        }
        GroupConsumerSeekTerminalFailureKind::Transport => GroupConsumerSeekErrorKind::Transport,
        GroupConsumerSeekTerminalFailureKind::BrokerRejected => {
            GroupConsumerSeekErrorKind::BrokerRejected
        }
        GroupConsumerSeekTerminalFailureKind::Compatibility => {
            GroupConsumerSeekErrorKind::Compatibility
        }
        GroupConsumerSeekTerminalFailureKind::InvalidResponse => {
            GroupConsumerSeekErrorKind::InvalidResponse
        }
        GroupConsumerSeekTerminalFailureKind::ResponseTooLarge => {
            GroupConsumerSeekErrorKind::ResponseTooLarge
        }
        GroupConsumerSeekTerminalFailureKind::AssignmentLost => {
            GroupConsumerSeekErrorKind::AssignmentLost
        }
        GroupConsumerSeekTerminalFailureKind::HostUnavailable => {
            GroupConsumerSeekErrorKind::HostUnavailable
        }
        GroupConsumerSeekTerminalFailureKind::InternalInvariant => {
            GroupConsumerSeekErrorKind::InternalInvariant
        }
    };
    GroupConsumerSeekError {
        kind,
        broker_code: failure.broker_code,
    }
}
