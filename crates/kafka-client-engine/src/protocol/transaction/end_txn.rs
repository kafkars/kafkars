//! Generated v3 `EndTxn` construction and lossless terminal normalization.

use kafka_wire::{EndTxnRequest, EndTxnResponse};

use super::{TransactionBrokerError, broker_error::transaction_broker_error};

/// The exact transaction terminal requested from the coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EndTxnDisposition {
    Commit,
    Abort,
}

/// Builds one v3 request without coordinator routing or retry policy.
pub(crate) fn end_txn_v3_request(
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    disposition: EndTxnDisposition,
) -> EndTxnRequest {
    let mut request = EndTxnRequest::default();
    request.transactional_id = transactional_id.into();
    request.producer_id = producer_id;
    request.producer_epoch = producer_epoch;
    request.committed = disposition == EndTxnDisposition::Commit;
    request
}

/// One v3 coordinator terminal with its exact signed broker fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EndTxnOutcome {
    Succeeded {
        throttle_time_ms: u32,
    },
    Rejected {
        throttle_time_ms: u32,
        error: TransactionBrokerError,
    },
}

/// Generated response facts not representable by exact v3 semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EndTxnResponseFailure {
    NegativeThrottleTime {
        actual: i32,
    },
    UnexpectedProducerIdentity {
        producer_id: i64,
        producer_epoch: i16,
    },
}

/// Normalizes one v3 terminal without adding retry, fatal, or abort policy.
pub(crate) fn normalize_end_txn_v3_response(
    response: &EndTxnResponse,
) -> Result<EndTxnOutcome, EndTxnResponseFailure> {
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        EndTxnResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    if response.producer_id != -1 || response.producer_epoch != -1 {
        return Err(EndTxnResponseFailure::UnexpectedProducerIdentity {
            producer_id: response.producer_id,
            producer_epoch: response.producer_epoch,
        });
    }
    Ok(match transaction_broker_error(response.error_code) {
        Some(error) => EndTxnOutcome::Rejected {
            throttle_time_ms,
            error,
        },
        None => EndTxnOutcome::Succeeded { throttle_time_ms },
    })
}
