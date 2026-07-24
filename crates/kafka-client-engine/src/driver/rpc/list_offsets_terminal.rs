//! Semantic settlement of one versioned, fenced `ListOffsets` terminal.

use kafka_client_core::{AssignedConsumerInput, Moment, NextFetchOffset, PositionFence};
use kafka_driver::{ApiVersion, RequestError};
use kafka_wire::ListOffsetsResponse;

use crate::protocol::consumer::{
    ListOffsetsIsolation, ListOffsetsOutcome, normalize_list_offsets_response, throttle_ticks,
};

/// Copyable terminal fact retained until deterministic core accepts it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PositionResolutionTerminal {
    fence: PositionFence,
    now: Moment,
    outcome: PositionResolutionOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PositionResolutionOutcome {
    Resolved {
        next_offset: NextFetchOffset,
        throttle_ticks: u64,
    },
    Failed,
}

impl PositionResolutionTerminal {
    pub(crate) const fn failed(fence: PositionFence, now: Moment) -> Self {
        Self {
            fence,
            now,
            outcome: PositionResolutionOutcome::Failed,
        }
    }

    pub(crate) const fn fence(self) -> PositionFence {
        self.fence
    }

    pub(crate) const fn core_input(self) -> AssignedConsumerInput {
        match self.outcome {
            PositionResolutionOutcome::Resolved {
                next_offset,
                throttle_ticks,
            } => AssignedConsumerInput::PositionResolved {
                fence: self.fence,
                next_offset,
                now: self.now,
                throttle_ticks,
            },
            PositionResolutionOutcome::Failed => AssignedConsumerInput::PositionResolutionFailed {
                fence: self.fence,
                now: self.now,
            },
        }
    }
}

pub(super) fn normalize_position_terminal(
    fence: PositionFence,
    topic: &str,
    isolation: ListOffsetsIsolation,
    now: Moment,
    selected_version: Option<ApiVersion>,
    result: Result<ListOffsetsResponse, RequestError>,
) -> PositionResolutionTerminal {
    let Ok(response) = result else {
        return PositionResolutionTerminal::failed(fence, now);
    };
    let Some(selected_version) = selected_version else {
        return PositionResolutionTerminal::failed(fence, now);
    };
    if selected_version.value() < minimum_version(isolation) {
        return PositionResolutionTerminal::failed(fence, now);
    }
    let Ok(normalized) = normalize_list_offsets_response(
        topic,
        fence.partition().partition(),
        selected_version.value(),
        &response,
    ) else {
        return PositionResolutionTerminal::failed(fence, now);
    };
    let ListOffsetsOutcome::Resolved(position) = normalized.outcome() else {
        return PositionResolutionTerminal::failed(fence, now);
    };
    let Some(throttle_ticks) = throttle_ticks(normalized.throttle_time_ms()) else {
        return PositionResolutionTerminal::failed(fence, now);
    };
    PositionResolutionTerminal {
        fence,
        now,
        outcome: PositionResolutionOutcome::Resolved {
            next_offset: position.next_offset(),
            throttle_ticks,
        },
    }
}

const fn minimum_version(isolation: ListOffsetsIsolation) -> i16 {
    match isolation {
        ListOffsetsIsolation::ReadUncommitted => 1,
        ListOffsetsIsolation::ReadCommitted => 2,
    }
}
