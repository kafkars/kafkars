//! Generated v3-v4 `AddOffsetsToTxn` adaptation.
//!
//! Both versions retain identical explicit enrollment fields. Version 3 supports
//! Kafka 3.7; version 4 additionally permits `TRANSACTION_ABORTABLE`. Neither
//! relies on `TxnOffsetCommit` v5 transaction-v2 fusion.

use kafka_wire::{AddOffsetsToTxnRequest, AddOffsetsToTxnResponse};

use super::{TransactionBrokerError, broker_error::transaction_broker_error};

/// Request facts that cannot safely enter the generated v4 shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddOffsetsToTxnRequestFailure {
    EmptyTransactionalId,
    InvalidProducerId { actual: i64 },
    InvalidProducerEpoch { actual: i16 },
    EmptyGroupId,
}

/// Builds one v4 request without coordinator routing or retry policy.
pub(crate) fn add_offsets_to_txn_v4_request(
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id: &str,
) -> Result<AddOffsetsToTxnRequest, AddOffsetsToTxnRequestFailure> {
    if transactional_id.is_empty() {
        return Err(AddOffsetsToTxnRequestFailure::EmptyTransactionalId);
    }
    if producer_id < 0 {
        return Err(AddOffsetsToTxnRequestFailure::InvalidProducerId {
            actual: producer_id,
        });
    }
    if producer_epoch < 0 {
        return Err(AddOffsetsToTxnRequestFailure::InvalidProducerEpoch {
            actual: producer_epoch,
        });
    }
    if group_id.is_empty() {
        return Err(AddOffsetsToTxnRequestFailure::EmptyGroupId);
    }
    let mut request = AddOffsetsToTxnRequest::default();
    request.transactional_id = transactional_id.into();
    request.producer_id = producer_id;
    request.producer_epoch = producer_epoch;
    request.group_id = group_id.into();
    Ok(request)
}

/// One exact v4 coordinator outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddOffsetsToTxnOutcome {
    Added {
        throttle_time_ms: u32,
    },
    Rejected {
        throttle_time_ms: u32,
        error: TransactionBrokerError,
    },
}

/// Generated response facts not representable by exact v4 semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddOffsetsToTxnResponseFailure {
    NegativeThrottleTime { actual: i32 },
}

/// Normalizes one v4 response without adding retry or transaction policy.
pub(crate) fn normalize_add_offsets_to_txn_v4_response(
    response: &AddOffsetsToTxnResponse,
) -> Result<AddOffsetsToTxnOutcome, AddOffsetsToTxnResponseFailure> {
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        AddOffsetsToTxnResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    Ok(match transaction_broker_error(response.error_code) {
        Some(error) => AddOffsetsToTxnOutcome::Rejected {
            throttle_time_ms,
            error,
        },
        None => AddOffsetsToTxnOutcome::Added { throttle_time_ms },
    })
}
