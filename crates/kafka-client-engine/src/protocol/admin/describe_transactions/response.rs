//! Strict one-ID `DescribeTransactions` response normalization.

use kafka_wire::DescribeTransactionsResponse;

use super::{
    NormalizedDescribeTransactionBrokerError, NormalizedDescribeTransactionResult,
    NormalizedDescribeTransactionsResponse,
    correlation::correlated_state,
    materialize::materialize_success,
    retention::{ensure_limit, error_charge, source_success_charge},
    validation::{validate_error_payload, validate_success_payload},
};

/// Structural, scalar, allocation, or retained-capacity response failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeTransactionsProtocolFailure {
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    UnexpectedTransactionStateCount {
        actual: usize,
    },
    UnexpectedTransactionalId,
    SuccessPayloadWithBrokerError {
        field: &'static str,
    },
    EmptyTransactionState,
    TransactionStateTooLong {
        actual: usize,
        max: usize,
    },
    InvalidTransactionStartTime {
        actual: i64,
    },
    TooManyTopics {
        actual: usize,
        max: usize,
    },
    EmptyTopic,
    TopicTooLong {
        actual: usize,
        max: usize,
    },
    EmptyPartitions,
    TooManyPartitions {
        actual: usize,
        max: usize,
    },
    NegativePartition {
        actual: i32,
    },
    DuplicateTopic,
    DuplicatePartition {
        actual: i32,
    },
    TopicBytesExceeded {
        required: usize,
        max: usize,
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
pub(crate) fn normalize_describe_transactions_response(
    transactional_id: &str,
    selected_version: i16,
    response: &DescribeTransactionsResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeTransactionsResponse, DescribeTransactionsProtocolFailure> {
    if selected_version != 0 {
        return Err(DescribeTransactionsProtocolFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        DescribeTransactionsProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let state = correlated_state(transactional_id, response)?;
    if state.error_code != 0 {
        validate_error_payload(state)?;
        let required = error_charge();
        ensure_limit(required, retained_limit)?;
        return Ok(NormalizedDescribeTransactionsResponse::new(
            throttle_time_ms,
            NormalizedDescribeTransactionResult::BrokerFailed(
                NormalizedDescribeTransactionBrokerError::new(state.error_code),
            ),
            required,
        ));
    }
    validate_success_payload(state)?;
    let required = source_success_charge(state).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    let (description, retained_bytes) = materialize_success(state, required, retained_limit)?;
    Ok(NormalizedDescribeTransactionsResponse::new(
        throttle_time_ms,
        NormalizedDescribeTransactionResult::Described(description),
        retained_bytes,
    ))
}
