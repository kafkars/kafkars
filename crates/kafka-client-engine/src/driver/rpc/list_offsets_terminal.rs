//! Semantic settlement of one versioned, fenced `ListOffsets` terminal.

use kafka_client_core::{
    AssignedConsumerInput, Moment, NextFetchOffset, PartitionIndex, PositionFence,
    PositionResolutionAttemptFailure,
};
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
    Failed(PositionResolutionAttemptFailure),
}

/// Fence-independent normalized result of one exact `ListOffsets` response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListOffsetsResolution {
    /// Kafka resolved the requested position and reported a nonnegative throttle.
    Resolved {
        next_offset: NextFetchOffset,
        throttle_time_ms: u32,
    },
    /// Driver, protocol, or Kafka execution failed exactly once.
    Failed(PositionResolutionAttemptFailure),
}

impl PositionResolutionTerminal {
    pub(crate) const fn failed(
        fence: PositionFence,
        now: Moment,
        failure: PositionResolutionAttemptFailure,
    ) -> Self {
        Self {
            fence,
            now,
            outcome: PositionResolutionOutcome::Failed(failure),
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
            PositionResolutionOutcome::Failed(failure) => {
                AssignedConsumerInput::PositionResolutionFailed {
                    fence: self.fence,
                    now: self.now,
                    failure,
                }
            }
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
    let normalized = normalize_list_offsets_terminal(
        topic,
        fence.partition().partition(),
        isolation,
        selected_version,
        result,
    );
    let outcome = match normalized {
        ListOffsetsResolution::Resolved {
            next_offset,
            throttle_time_ms,
        } => {
            let Some(throttle_ticks) = throttle_ticks(throttle_time_ms) else {
                return PositionResolutionTerminal::failed(
                    fence,
                    now,
                    PositionResolutionAttemptFailure::InvalidResponse,
                );
            };
            PositionResolutionOutcome::Resolved {
                next_offset,
                throttle_ticks,
            }
        }
        ListOffsetsResolution::Failed(failure) => PositionResolutionOutcome::Failed(failure),
    };
    PositionResolutionTerminal {
        fence,
        now,
        outcome,
    }
}

pub(crate) fn normalize_list_offsets_terminal(
    topic: &str,
    partition: PartitionIndex,
    isolation: ListOffsetsIsolation,
    selected_version: Option<ApiVersion>,
    result: Result<ListOffsetsResponse, RequestError>,
) -> ListOffsetsResolution {
    let response = match result {
        Ok(response) => response,
        Err(failure) => {
            return ListOffsetsResolution::Failed(
                super::list_offsets_failure::classify_request_error(&failure),
            );
        }
    };
    let Some(selected_version) = selected_version else {
        return ListOffsetsResolution::Failed(PositionResolutionAttemptFailure::Compatibility);
    };
    if selected_version.value() < minimum_version(isolation) {
        return ListOffsetsResolution::Failed(PositionResolutionAttemptFailure::Compatibility);
    }
    let normalized = match normalize_list_offsets_response(
        topic,
        partition,
        selected_version.value(),
        &response,
    ) {
        Ok(normalized) => normalized,
        Err(failure) => {
            return ListOffsetsResolution::Failed(
                super::list_offsets_failure::classify_response_failure(failure),
            );
        }
    };
    let position = match normalized.outcome() {
        ListOffsetsOutcome::Resolved(position) => position,
        ListOffsetsOutcome::BrokerError { code } => {
            return ListOffsetsResolution::Failed(PositionResolutionAttemptFailure::Broker(code));
        }
    };
    ListOffsetsResolution::Resolved {
        next_offset: position.next_offset(),
        throttle_time_ms: normalized.throttle_time_ms(),
    }
}

const fn minimum_version(isolation: ListOffsetsIsolation) -> i16 {
    match isolation {
        ListOffsetsIsolation::ReadUncommitted => 1,
        ListOffsetsIsolation::ReadCommitted => 2,
    }
}
