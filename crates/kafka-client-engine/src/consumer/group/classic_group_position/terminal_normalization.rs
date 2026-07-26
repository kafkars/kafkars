//! Bounded raw `OffsetFetch` normalization into one deterministic core fact.

use crate::{
    driver::{
        GroupPositionOffsetFetchDriverFailureKind, GroupPositionOffsetFetchTerminal,
        GroupPositionOffsetFetchTerminalFact,
    },
    protocol::consumer::{
        GroupOffsetFetchCorrelation, GroupOffsetFetchPartitionValueRef,
        GroupOffsetFetchProtocolFailure, normalize_group_offset_fetch_response,
    },
};
use kafka_client_core::{
    GroupPositionBatch, GroupPositionBootstrapFetchFailure, GroupPositionBootstrapInput,
    GroupPositionBootstrapMachine, GroupPositionBrokerError, GroupPositionPartitionFact, Moment,
    NextFetchOffset,
};

use super::{CLASSIC_GROUP_POSITION_RESULT_RETAINED_BYTES, ClassicGroupPositionExecutionError};

pub(super) struct ClassicGroupPositionNormalizedTerminal {
    input: GroupPositionBootstrapInput,
    spare_buffer: Option<Vec<GroupPositionPartitionFact>>,
}

type NormalizeFailure = (
    ClassicGroupPositionExecutionError,
    Vec<GroupPositionPartitionFact>,
);
type NormalizeResult = Result<ClassicGroupPositionNormalizedTerminal, NormalizeFailure>;

impl ClassicGroupPositionNormalizedTerminal {
    const fn with_spare(
        input: GroupPositionBootstrapInput,
        spare_buffer: Vec<GroupPositionPartitionFact>,
    ) -> Self {
        Self {
            input,
            spare_buffer: Some(spare_buffer),
        }
    }

    const fn with_batch(input: GroupPositionBootstrapInput) -> Self {
        Self {
            input,
            spare_buffer: None,
        }
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        GroupPositionBootstrapInput,
        Option<Vec<GroupPositionPartitionFact>>,
    ) {
        (self.input, self.spare_buffer)
    }
}

pub(super) fn normalize_terminal(
    machine: &GroupPositionBootstrapMachine,
    correlation: &GroupOffsetFetchCorrelation,
    terminal: &GroupPositionOffsetFetchTerminal,
    now: Moment,
    facts: Vec<GroupPositionPartitionFact>,
) -> NormalizeResult {
    if !facts.is_empty() || facts.capacity() < machine.partitions().len() {
        return Err((ClassicGroupPositionExecutionError::ResultBuffer, facts));
    }
    if correlation.partition_count() != machine.partitions().len() {
        return Err((
            ClassicGroupPositionExecutionError::TerminalCorrelation,
            facts,
        ));
    }
    normalize_fact(
        machine,
        correlation,
        terminal.fact(),
        terminal.key().fence(),
        now,
        facts,
    )
}

fn normalize_fact(
    machine: &GroupPositionBootstrapMachine,
    correlation: &GroupOffsetFetchCorrelation,
    fact: GroupPositionOffsetFetchTerminalFact<'_>,
    fence: kafka_client_core::GroupPositionFence,
    now: Moment,
    mut facts: Vec<GroupPositionPartitionFact>,
) -> NormalizeResult {
    match fact {
        GroupPositionOffsetFetchTerminalFact::Failed { kind } => {
            Ok(ClassicGroupPositionNormalizedTerminal::with_spare(
                driver_failure(fence, now, kind),
                facts,
            ))
        }
        GroupPositionOffsetFetchTerminalFact::Response {
            selected_version,
            response,
        } => {
            let Some(selected_version) = selected_version else {
                return Ok(ClassicGroupPositionNormalizedTerminal::with_spare(
                    fetch_failed(
                        fence,
                        now,
                        GroupPositionBootstrapFetchFailure::InvalidResponse,
                    ),
                    facts,
                ));
            };
            let normalized = match normalize_group_offset_fetch_response(
                correlation,
                response,
                selected_version,
                CLASSIC_GROUP_POSITION_RESULT_RETAINED_BYTES,
            ) {
                Ok(normalized) => normalized,
                Err(error) => {
                    return Ok(ClassicGroupPositionNormalizedTerminal::with_spare(
                        protocol_failure(fence, now, error),
                        facts,
                    ));
                }
            };
            if let Some(code) = normalized.top_level_error() {
                return Ok(ClassicGroupPositionNormalizedTerminal::with_spare(
                    GroupPositionBootstrapInput::BrokerRejected {
                        fence,
                        now,
                        error: GroupPositionBrokerError::new(code),
                    },
                    facts,
                ));
            }
            if normalized.entries().len() != machine.partitions().len() {
                return Err((
                    ClassicGroupPositionExecutionError::TerminalCorrelation,
                    facts,
                ));
            }
            for (partition, value) in machine
                .partitions()
                .iter()
                .copied()
                .zip(normalized.entries())
            {
                let fact = match value {
                    GroupOffsetFetchPartitionValueRef::Fetched {
                        committed_offset: Some(offset),
                        ..
                    } => {
                        let Some(offset) = NextFetchOffset::try_from_raw(*offset) else {
                            return Err((
                                ClassicGroupPositionExecutionError::TerminalCorrelation,
                                facts,
                            ));
                        };
                        GroupPositionPartitionFact::committed(partition, offset)
                    }
                    GroupOffsetFetchPartitionValueRef::Fetched {
                        committed_offset: None,
                        ..
                    } => GroupPositionPartitionFact::missing(partition),
                    GroupOffsetFetchPartitionValueRef::Rejected { code } => {
                        GroupPositionPartitionFact::rejected(
                            partition,
                            GroupPositionBrokerError::new(*code),
                        )
                    }
                };
                facts.push(fact);
            }
            Ok(ClassicGroupPositionNormalizedTerminal::with_batch(
                GroupPositionBootstrapInput::OffsetsFetched {
                    fence,
                    now,
                    batch: GroupPositionBatch::new(normalized.throttle_time_ms(), facts),
                },
            ))
        }
    }
}

fn driver_failure(
    fence: kafka_client_core::GroupPositionFence,
    now: Moment,
    kind: GroupPositionOffsetFetchDriverFailureKind,
) -> GroupPositionBootstrapInput {
    match kind {
        GroupPositionOffsetFetchDriverFailureKind::DeadlineElapsed => {
            GroupPositionBootstrapInput::DeadlineElapsed { fence, now }
        }
        GroupPositionOffsetFetchDriverFailureKind::InvalidResponse => fetch_failed(
            fence,
            now,
            GroupPositionBootstrapFetchFailure::InvalidResponse,
        ),
        GroupPositionOffsetFetchDriverFailureKind::Compatibility => fetch_failed(
            fence,
            now,
            GroupPositionBootstrapFetchFailure::Compatibility,
        ),
        GroupPositionOffsetFetchDriverFailureKind::Transport => {
            fetch_failed(fence, now, GroupPositionBootstrapFetchFailure::Transport)
        }
    }
}

const fn protocol_failure(
    fence: kafka_client_core::GroupPositionFence,
    now: Moment,
    error: GroupOffsetFetchProtocolFailure,
) -> GroupPositionBootstrapInput {
    let failure = match error {
        GroupOffsetFetchProtocolFailure::UnsupportedApiVersion { .. } => {
            GroupPositionBootstrapFetchFailure::Compatibility
        }
        GroupOffsetFetchProtocolFailure::RetainedBytes => {
            GroupPositionBootstrapFetchFailure::ResponseTooLarge
        }
        _ => GroupPositionBootstrapFetchFailure::InvalidResponse,
    };
    fetch_failed(fence, now, failure)
}

const fn fetch_failed(
    fence: kafka_client_core::GroupPositionFence,
    now: Moment,
    failure: GroupPositionBootstrapFetchFailure,
) -> GroupPositionBootstrapInput {
    GroupPositionBootstrapInput::FetchFailed {
        fence,
        now,
        failure,
    }
}
