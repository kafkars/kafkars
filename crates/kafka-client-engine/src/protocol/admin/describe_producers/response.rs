//! Strict one-target `DescribeProducers` response normalization.

use kafka_client_core::AdminDescribeProducerTarget;
use kafka_wire::DescribeProducersResponse;

use super::{
    NormalizedDescribeProducerBrokerError, NormalizedDescribeProducerResult,
    NormalizedDescribeProducersResponse,
    correlation::correlated_partition,
    retention::{diagnostic_charge, ensure_limit, retained_diagnostic},
    validation::normalized_states,
};

/// Structural, scalar, allocation, or retained-capacity response failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeProducersProtocolFailure {
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    UnexpectedTopicCount {
        actual: usize,
    },
    UnexpectedTopic,
    UnexpectedPartitionCount {
        actual: usize,
    },
    NegativePartition {
        actual: i32,
    },
    UnexpectedPartition {
        actual: i32,
    },
    ProducerStatesWithPartitionError {
        actual: usize,
    },
    DiagnosticOnSuccess,
    TooManyProducerStates {
        actual: usize,
        max: usize,
    },
    NegativeProducerId {
        actual: i64,
    },
    NegativeProducerEpoch {
        actual: i32,
    },
    InvalidLastSequence {
        actual: i32,
    },
    InvalidLastTimestamp {
        actual: i64,
    },
    NegativeCoordinatorEpoch {
        actual: i32,
    },
    InvalidCurrentTransactionStartOffset {
        actual: i64,
    },
    DuplicateProducerId {
        actual: i64,
    },
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Correlates and normalizes one exact selected-v0 response.
pub(crate) fn normalize_describe_producers_response(
    target: &AdminDescribeProducerTarget,
    selected_version: i16,
    response: &DescribeProducersResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeProducersResponse, DescribeProducersProtocolFailure> {
    if selected_version != 0 {
        return Err(DescribeProducersProtocolFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        DescribeProducersProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let partition = correlated_partition(target, response)?;
    let (result, retained_bytes) = if partition.error_code == 0 {
        if partition.error_message.is_some() {
            return Err(DescribeProducersProtocolFailure::DiagnosticOnSuccess);
        }
        let (states, retained) = normalized_states(&partition.active_producers, retained_limit)?;
        (
            NormalizedDescribeProducerResult::Described(states),
            retained,
        )
    } else {
        if !partition.active_producers.is_empty() {
            return Err(
                DescribeProducersProtocolFailure::ProducerStatesWithPartitionError {
                    actual: partition.active_producers.len(),
                },
            );
        }
        let (message, truncated, retained) =
            if let Some(message) = partition.error_message.as_deref() {
                let (message, truncated, retained) = retained_diagnostic(message, retained_limit)?;
                (Some(message), truncated, retained)
            } else {
                let retained = diagnostic_charge(0).unwrap_or(usize::MAX);
                ensure_limit(retained, retained_limit)?;
                (None, false, retained)
            };
        (
            NormalizedDescribeProducerResult::BrokerFailed(
                NormalizedDescribeProducerBrokerError::new(
                    partition.error_code,
                    message,
                    truncated,
                ),
            ),
            retained,
        )
    };
    Ok(NormalizedDescribeProducersResponse::new(
        throttle_time_ms,
        result,
        retained_bytes,
    ))
}
