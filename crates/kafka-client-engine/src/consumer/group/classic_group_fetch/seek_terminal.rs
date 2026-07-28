//! Exhaustive seek terminal translation from deterministic core effects.

use kafka_client_core::{
    AssignedConsumerEffect, PositionFence, PositionResolutionAttemptFailure,
    PositionResolutionFailure,
};

use crate::consumer::group_seek::{
    GroupConsumerSeekTerminal, GroupConsumerSeekTerminalFailure,
    GroupConsumerSeekTerminalFailureKind,
};

pub(super) fn seek_terminal(
    fence: PositionFence,
    effect: AssignedConsumerEffect,
) -> Option<GroupConsumerSeekTerminal> {
    match effect {
        AssignedConsumerEffect::FetchReady {
            fence: fetch_fence, ..
        } if fetch_fence.position() == fence => Some(GroupConsumerSeekTerminal::Succeeded),
        AssignedConsumerEffect::ArmPositionThrottle {
            fence: supplied, ..
        } if supplied == fence => Some(GroupConsumerSeekTerminal::Succeeded),
        AssignedConsumerEffect::PositionResolutionFailed {
            fence: supplied,
            failure,
        } if supplied == fence => Some(position_failure(failure)),
        _ => None,
    }
}

fn position_failure(failure: PositionResolutionFailure) -> GroupConsumerSeekTerminal {
    match failure {
        PositionResolutionFailure::DeadlineElapsed => {
            failed(GroupConsumerSeekTerminalFailureKind::DeadlineElapsed, None)
        }
        PositionResolutionFailure::ThrottleDeadlineOverflow => failed(
            GroupConsumerSeekTerminalFailureKind::InternalInvariant,
            None,
        ),
        PositionResolutionFailure::Attempt(attempt) => attempt_failure(attempt),
    }
}

fn attempt_failure(failure: PositionResolutionAttemptFailure) -> GroupConsumerSeekTerminal {
    let (kind, broker_code) = match failure {
        PositionResolutionAttemptFailure::DeadlineElapsed => {
            (GroupConsumerSeekTerminalFailureKind::DeadlineElapsed, None)
        }
        PositionResolutionAttemptFailure::DriverRejected => {
            (GroupConsumerSeekTerminalFailureKind::DriverRejected, None)
        }
        PositionResolutionAttemptFailure::Transport => {
            (GroupConsumerSeekTerminalFailureKind::Transport, None)
        }
        PositionResolutionAttemptFailure::Broker(code) => (
            GroupConsumerSeekTerminalFailureKind::BrokerRejected,
            Some(code.get()),
        ),
        PositionResolutionAttemptFailure::Compatibility => {
            (GroupConsumerSeekTerminalFailureKind::Compatibility, None)
        }
        PositionResolutionAttemptFailure::InvalidResponse => {
            (GroupConsumerSeekTerminalFailureKind::InvalidResponse, None)
        }
        PositionResolutionAttemptFailure::ResponseTooLarge => {
            (GroupConsumerSeekTerminalFailureKind::ResponseTooLarge, None)
        }
    };
    failed(kind, broker_code)
}

const fn failed(
    kind: GroupConsumerSeekTerminalFailureKind,
    broker_code: Option<i16>,
) -> GroupConsumerSeekTerminal {
    GroupConsumerSeekTerminal::Failed(GroupConsumerSeekTerminalFailure { kind, broker_code })
}
