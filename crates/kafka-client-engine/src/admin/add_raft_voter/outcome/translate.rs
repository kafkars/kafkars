//! Exhaustive core-to-engine voter-addition terminal translation.

use kafka_client_core::{
    AddRaftVoterFailureKind as CoreFailureKind, AddRaftVoterTerminal as CoreTerminal,
    DeliveryStatus as CoreDeliveryStatus,
};

use super::{
    AddRaftVoterBrokerError, AddRaftVoterDeliveryStatus, AddRaftVoterFailure,
    AddRaftVoterFailureKind, AddRaftVoterOutcome,
};
use crate::admin::add_raft_voter::AddRaftVoterResult;

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AddRaftVoterOutcome {
    match terminal {
        CoreTerminal::Added(success) => AddRaftVoterOutcome::Added(AddRaftVoterResult {
            throttle_time_ms: success.throttle_time_ms(),
        }),
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
            AddRaftVoterOutcome::BrokerRejected(AddRaftVoterBrokerError {
                throttle_time_ms,
                code,
                message,
                message_truncated,
            })
        }
        CoreTerminal::Failed(failure) => AddRaftVoterOutcome::Failed(AddRaftVoterFailure {
            kind: failure_kind(failure.kind()),
            delivery: delivery(failure.delivery()),
        }),
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AddRaftVoterFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AddRaftVoterFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AddRaftVoterFailureKind::DriverRejected,
        CoreFailureKind::Transport => AddRaftVoterFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AddRaftVoterFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AddRaftVoterFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AddRaftVoterFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> AddRaftVoterDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AddRaftVoterDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AddRaftVoterDeliveryStatus::PossiblySent,
    }
}
