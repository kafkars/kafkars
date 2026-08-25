//! Core input and retained-store shape derived from normalized Fetch outcomes.

use kafka_client_core::{AssignedConsumerInput, FetchFailure, FetchFence, FetchRecords, Moment};

use crate::{
    driver::PartitionFetchRequest,
    protocol::fetch::{FetchOutcomeFailureClass, FetchSessionUpdate},
};

use super::{
    super::fetch_store::FetchStageKind,
    terminal::{FetchTerminalAction, FetchTerminalFact, TerminalStorage},
    terminal_proposal::FetchTerminalProposal,
};

pub(super) const fn core_outcome_failure(failure: FetchOutcomeFailureClass) -> FetchFailure {
    match failure {
        FetchOutcomeFailureClass::DriverRejected => FetchFailure::DriverRejected,
        FetchOutcomeFailureClass::Compatibility => FetchFailure::Compatibility,
        FetchOutcomeFailureClass::InvalidResponse => FetchFailure::InvalidResponse,
        FetchOutcomeFailureClass::ResponseTooLarge => FetchFailure::ResponseTooLarge,
    }
}

pub(super) fn staged_fact(
    request: PartitionFetchRequest,
    hard_output_bytes: usize,
    observed_at: Moment,
    kind: FetchStageKind,
    fence: FetchFence,
    session: FetchSessionUpdate,
) -> FetchTerminalProposal {
    let (input, storage, broker) = match kind {
        FetchStageKind::BrokerFailure(failure) => (
            AssignedConsumerInput::FetchFailed {
                fence,
                failure: FetchFailure::Broker(failure.code()),
            },
            TerminalStorage::NonDelivery(fence),
            Some(failure),
        ),
        FetchStageKind::Empty(next_offset, throttle_ticks) => (
            AssignedConsumerInput::FetchAdvanced {
                fence,
                records: FetchRecords::NoApplicationRecords,
                next_offset,
                now: observed_at,
                throttle_ticks,
            },
            TerminalStorage::NonDelivery(fence),
            None,
        ),
        FetchStageKind::Progress(next_offset, throttle_ticks) => (
            AssignedConsumerInput::FetchAdvanced {
                fence,
                records: FetchRecords::ProgressOnlyDelivery,
                next_offset,
                now: observed_at,
                throttle_ticks,
            },
            TerminalStorage::Deliverable(fence, next_offset),
            None,
        ),
        FetchStageKind::Deliverable(next_offset, throttle_ticks) => (
            AssignedConsumerInput::FetchAdvanced {
                fence,
                records: FetchRecords::Deliverable,
                next_offset,
                now: observed_at,
                throttle_ticks,
            },
            TerminalStorage::Deliverable(fence, next_offset),
            None,
        ),
    };
    FetchTerminalProposal::new(
        FetchTerminalFact {
            request,
            hard_output_bytes,
            action: FetchTerminalAction::Apply(input),
            storage,
            session,
        },
        broker,
    )
}
