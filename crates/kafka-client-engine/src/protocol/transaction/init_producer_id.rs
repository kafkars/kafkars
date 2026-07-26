//! Generated transactional `InitProducerId` request and response normalization.

use core::num::NonZeroI16;

use kafka_wire::{InitProducerIdRequest, InitProducerIdResponse};
use kafka_wire_core::StrBytes;

const NO_PRODUCER_ID: i64 = -1;
const NO_PRODUCER_EPOCH: i16 = -1;
const INVALID_PRODUCER_EPOCH: i16 = 47;
const PRODUCER_FENCED: i16 = 90;

/// Broker-issued nonnegative producer identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionInitIdentity {
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
}

/// Whether Kafka's exact rejection fences the transactional owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitBrokerCategory {
    Fenced,
    Rejected,
}

/// Lossless generated-response failure before deterministic settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitResponseFailure {
    Broker {
        code: NonZeroI16,
        category: TransactionInitBrokerCategory,
    },
    InvalidIdentity,
}

/// Builds one initial transactional identity request without epoch-bump intent.
#[expect(
    clippy::cast_possible_wrap,
    reason = "the sole engine admission validates the timeout against i32::MAX before retention"
)]
pub(crate) fn transaction_init_request(
    transactional_id: &str,
    transaction_timeout_ms: u32,
) -> InitProducerIdRequest {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = Some(StrBytes::from(transactional_id));
    request.transaction_timeout_ms = transaction_timeout_ms as i32;
    request.producer_id = NO_PRODUCER_ID;
    request.producer_epoch = NO_PRODUCER_EPOCH;
    request.enable2_pc = false;
    request.keep_prepared_txn = false;
    request
}

/// Normalizes one response without coordinator, retry, or fencing policy.
pub(crate) fn normalize_transaction_init_response(
    response: &InitProducerIdResponse,
) -> Result<TransactionInitIdentity, TransactionInitResponseFailure> {
    if let Some(code) = NonZeroI16::new(response.error_code) {
        let category = match code.get() {
            INVALID_PRODUCER_EPOCH | PRODUCER_FENCED => TransactionInitBrokerCategory::Fenced,
            _ => TransactionInitBrokerCategory::Rejected,
        };
        return Err(TransactionInitResponseFailure::Broker { code, category });
    }
    if response.producer_id < 0 || response.producer_epoch < 0 {
        return Err(TransactionInitResponseFailure::InvalidIdentity);
    }
    Ok(TransactionInitIdentity {
        producer_id: response.producer_id,
        producer_epoch: response.producer_epoch,
    })
}
