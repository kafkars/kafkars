//! Public polling states and exact route-refresh classification for group commits.

use kafka_client_core::{GroupOffsetCommitInput, GroupOffsetCommitPartitionResult, OperationId};

pub(super) fn needs_coordinator_refresh(input: &GroupOffsetCommitInput) -> bool {
    matches!(
        input,
        GroupOffsetCommitInput::BrokerResponded { outcomes, .. }
            if outcomes.iter().any(|outcome| {
                matches!(
                    outcome.result(),
                    GroupOffsetCommitPartitionResult::Rejected(error)
                        if matches!(error.code(), 15 | 16)
                )
            })
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::driver::rpc) enum RouteTokenDestination {
    Confirm,
    Refresh,
}

pub(in crate::driver::rpc) fn route_token_destination(
    input: &GroupOffsetCommitInput,
) -> RouteTokenDestination {
    if needs_coordinator_refresh(input) {
        RouteTokenDestination::Refresh
    } else {
        RouteTokenDestination::Confirm
    }
}

/// One bounded nonblocking observation without moving terminal ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitPoll {
    Idle,
    TerminalReady { operation_id: OperationId },
    ConfirmationPending { operation_id: OperationId },
}

/// State of the causal coordinator-refresh barrier for one settled commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitRefreshPoll {
    Ready,
    ReplacementReady,
    Submitted,
    Pending,
}

/// Why terminal input ownership could not begin core settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitBeginError {
    NoSettlement {
        supplied: OperationId,
    },
    ConfirmationPending {
        pending: OperationId,
    },
    OperationMismatch {
        settled: OperationId,
        supplied: OperationId,
    },
}

/// Why exact route-token confirmation could not finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitConfirmationError {
    NoPendingConfirmation {
        supplied: OperationId,
    },
    OperationMismatch {
        pending: OperationId,
        supplied: OperationId,
    },
}

/// Failed restoration still owns the exact normalized input.
#[must_use = "failed restoration still owns the exact group commit input"]
pub(crate) struct GroupOffsetCommitRestoreFailure {
    input: GroupOffsetCommitInput,
    error: GroupOffsetCommitRestoreError,
}

impl GroupOffsetCommitRestoreFailure {
    pub(in crate::driver::rpc) const fn new(
        input: GroupOffsetCommitInput,
        error: GroupOffsetCommitRestoreError,
    ) -> Self {
        Self { input, error }
    }

    pub(crate) fn into_parts(self) -> (GroupOffsetCommitInput, GroupOffsetCommitRestoreError) {
        (self.input, self.error)
    }
}

/// Why terminal input could not rejoin its pending route confirmation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitRestoreError {
    SettlementPresent {
        supplied: OperationId,
    },
    NoPendingConfirmation {
        supplied: OperationId,
    },
    OperationMismatch {
        pending: OperationId,
        supplied: OperationId,
    },
}
