//! Exhaustive core-to-engine metadata-quorum voter-removal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, RemoveRaftVoterFailureKind as CoreFailureKind,
    RemoveRaftVoterTerminal as CoreTerminal,
};

use super::{
    RemoveRaftVoterBrokerError, RemoveRaftVoterDeliveryStatus, RemoveRaftVoterFailure,
    RemoveRaftVoterFailureKind, RemoveRaftVoterOutcome,
};
use crate::admin::remove_raft_voter::RemoveRaftVoterResult;

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> RemoveRaftVoterOutcome {
    match terminal {
        CoreTerminal::Removed(success) => RemoveRaftVoterOutcome::Removed(RemoveRaftVoterResult {
            throttle_time_ms: success.throttle_time_ms(),
        }),
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
            RemoveRaftVoterOutcome::BrokerRejected(RemoveRaftVoterBrokerError {
                throttle_time_ms,
                code,
                message,
                message_truncated,
            })
        }
        CoreTerminal::Failed(failure) => RemoveRaftVoterOutcome::Failed(RemoveRaftVoterFailure {
            kind: failure_kind(failure.kind()),
            delivery: delivery(failure.delivery()),
        }),
    }
}

const fn failure_kind(kind: CoreFailureKind) -> RemoveRaftVoterFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => RemoveRaftVoterFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => RemoveRaftVoterFailureKind::DriverRejected,
        CoreFailureKind::Transport => RemoveRaftVoterFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => RemoveRaftVoterFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => RemoveRaftVoterFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => RemoveRaftVoterFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> RemoveRaftVoterDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => RemoveRaftVoterDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => RemoveRaftVoterDeliveryStatus::PossiblySent,
    }
}
