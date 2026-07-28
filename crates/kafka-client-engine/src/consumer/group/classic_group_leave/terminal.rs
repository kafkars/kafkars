//! Raw tracked-call normalization into one stable explicit-close terminal.

use kafka_client_core::Moment;

use crate::{
    clock::OperationDeadline,
    driver::{ClassicGroupLeaveDriverFailureKind, ClassicGroupLeaveResolution},
};

use super::{
    completion::{
        GroupConsumerCloseTerminal, GroupConsumerCloseTerminalFailure,
        GroupConsumerCloseTerminalFailureKind,
    },
    failure::classify_leave_request_error,
};

pub(super) const fn should_rediscover(
    deadline_elapsed: bool,
    replacement_used: bool,
    resolution: ClassicGroupLeaveResolution,
) -> bool {
    !deadline_elapsed
        && !replacement_used
        && matches!(
            resolution,
            ClassicGroupLeaveResolution::BrokerRejected(15 | 16)
                | ClassicGroupLeaveResolution::Failed {
                    kind: ClassicGroupLeaveDriverFailureKind::Transport,
                    definitely_not_sent: true,
                }
        )
}

pub(super) fn normalize_terminal(
    deadline: OperationDeadline,
    now: Moment,
    resolution: ClassicGroupLeaveResolution,
) -> GroupConsumerCloseTerminal {
    if deadline.core().is_elapsed_at(now) {
        return failure(GroupConsumerCloseTerminalFailureKind::DeadlineElapsed, None);
    }
    match resolution {
        ClassicGroupLeaveResolution::Succeeded => GroupConsumerCloseTerminal::Succeeded,
        ClassicGroupLeaveResolution::BrokerRejected(error_code) => failure(
            GroupConsumerCloseTerminalFailureKind::BrokerRejected,
            Some(error_code),
        ),
        ClassicGroupLeaveResolution::Failed { kind, .. } => {
            failure(classify_leave_request_error(kind), None)
        }
    }
}

const fn failure(
    kind: GroupConsumerCloseTerminalFailureKind,
    broker_code: Option<i16>,
) -> GroupConsumerCloseTerminal {
    GroupConsumerCloseTerminal::Failed(GroupConsumerCloseTerminalFailure { kind, broker_code })
}
