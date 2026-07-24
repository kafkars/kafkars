//! Closed classification of staged direct-consumer Fetch outcomes.

use kafka_client_core::NextFetchOffset;

use crate::protocol::fetch::{FetchBrokerFailure, FetchOutcome, RetainedFetchOutcome};

use super::{FetchSlot, FetchStoreFailure};

/// Stable facts discovered while staging one normalized terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchStageKind {
    BrokerFailure(FetchBrokerFailure),
    Empty(NextFetchOffset, u64),
    Deliverable(NextFetchOffset, u64),
}

pub(super) fn slot_kind(slot: &FetchSlot) -> Result<FetchStageKind, FetchStoreFailure> {
    stage_kind(
        slot.outcome
            .as_ref()
            .ok_or(FetchStoreFailure::InvalidState)?,
    )
}

pub(super) fn stage_kind(
    outcome: &RetainedFetchOutcome,
) -> Result<FetchStageKind, FetchStoreFailure> {
    match outcome.outcome() {
        FetchOutcome::BrokerFailure(failure) => Ok(FetchStageKind::BrokerFailure(*failure)),
        FetchOutcome::Success {
            next_offset,
            data_batches,
        } => {
            let next = NextFetchOffset::try_from_raw(*next_offset)
                .ok_or(FetchStoreFailure::InvalidNextOffset)?;
            let throttle = outcome
                .throttle_ticks()
                .ok_or(FetchStoreFailure::MissingThrottle)?;
            if data_batches.is_empty() {
                Ok(FetchStageKind::Empty(next, throttle))
            } else {
                Ok(FetchStageKind::Deliverable(next, throttle))
            }
        }
    }
}
