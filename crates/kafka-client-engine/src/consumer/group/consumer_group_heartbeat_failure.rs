//! Stable core failure mapping for API 68 driver and completion terminals.

use kafka_client_core::ConsumerGroupHeartbeatFailure;

use crate::driver::{
    ConsumerGroupHeartbeatCompletionError, ConsumerGroupHeartbeatDriverFailureKind,
};

use super::classic_group_leave::{
    GroupConsumerCloseTerminal, GroupConsumerCloseTerminalFailure,
    GroupConsumerCloseTerminalFailureKind,
};

pub(super) const fn completion_failure(
    error: ConsumerGroupHeartbeatCompletionError,
) -> ConsumerGroupHeartbeatFailure {
    match error {
        ConsumerGroupHeartbeatCompletionError::Closed => ConsumerGroupHeartbeatFailure::Execution,
        ConsumerGroupHeartbeatCompletionError::Consumed
        | ConsumerGroupHeartbeatCompletionError::Unknown => {
            ConsumerGroupHeartbeatFailure::InvalidResponse
        }
    }
}

pub(super) const fn driver_failure(
    failure: ConsumerGroupHeartbeatDriverFailureKind,
) -> ConsumerGroupHeartbeatFailure {
    match failure {
        ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed => {
            ConsumerGroupHeartbeatFailure::DeadlineElapsed
        }
        ConsumerGroupHeartbeatDriverFailureKind::Compatibility => {
            ConsumerGroupHeartbeatFailure::Compatibility
        }
        ConsumerGroupHeartbeatDriverFailureKind::Transport => {
            ConsumerGroupHeartbeatFailure::CoordinatorUnavailable
        }
        ConsumerGroupHeartbeatDriverFailureKind::DriverRejected => {
            ConsumerGroupHeartbeatFailure::Execution
        }
        ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse
        | ConsumerGroupHeartbeatDriverFailureKind::ResponseTooLarge => {
            ConsumerGroupHeartbeatFailure::InvalidResponse
        }
    }
}

pub(super) const fn completion_close_terminal(
    error: ConsumerGroupHeartbeatCompletionError,
) -> GroupConsumerCloseTerminal {
    close_failure(
        match error {
            ConsumerGroupHeartbeatCompletionError::Closed => {
                GroupConsumerCloseTerminalFailureKind::Transport
            }
            ConsumerGroupHeartbeatCompletionError::Consumed
            | ConsumerGroupHeartbeatCompletionError::Unknown => {
                GroupConsumerCloseTerminalFailureKind::InvalidResponse
            }
        },
        None,
    )
}

pub(super) const fn driver_close_terminal(
    failure: ConsumerGroupHeartbeatDriverFailureKind,
) -> GroupConsumerCloseTerminal {
    close_failure(
        match failure {
            ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed => {
                GroupConsumerCloseTerminalFailureKind::DeadlineElapsed
            }
            ConsumerGroupHeartbeatDriverFailureKind::Compatibility => {
                GroupConsumerCloseTerminalFailureKind::Compatibility
            }
            ConsumerGroupHeartbeatDriverFailureKind::Transport => {
                GroupConsumerCloseTerminalFailureKind::Transport
            }
            ConsumerGroupHeartbeatDriverFailureKind::DriverRejected => {
                GroupConsumerCloseTerminalFailureKind::DriverRejected
            }
            ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse => {
                GroupConsumerCloseTerminalFailureKind::InvalidResponse
            }
            ConsumerGroupHeartbeatDriverFailureKind::ResponseTooLarge => {
                GroupConsumerCloseTerminalFailureKind::ResponseTooLarge
            }
        },
        None,
    )
}

pub(super) const fn broker_close_terminal(error_code: i16) -> GroupConsumerCloseTerminal {
    close_failure(
        GroupConsumerCloseTerminalFailureKind::BrokerRejected,
        Some(error_code),
    )
}

const fn close_failure(
    kind: GroupConsumerCloseTerminalFailureKind,
    broker_code: Option<i16>,
) -> GroupConsumerCloseTerminal {
    GroupConsumerCloseTerminal::Failed(GroupConsumerCloseTerminalFailure { kind, broker_code })
}
