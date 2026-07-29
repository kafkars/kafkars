//! Stable Admin fencing facts normalized from one generated InitProducerId response.

use core::num::NonZeroI16;

use kafka_client_core::{Deadline, Moment};
use kafka_wire::InitProducerIdResponse;

use super::super::request_timeout::{RequestDeadlineError, remaining_timeout_ms};
use super::{TransactionInitResponseFailure, normalize_transaction_init_response};

/// One normalized per-transactional-ID fencing result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NormalizedFenceProducerResult {
    Fenced {
        producer_id: i64,
        producer_epoch: i16,
    },
    BrokerFailed {
        code: NonZeroI16,
    },
}

/// Stable response facts plus exact result bytes charged to the operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedFenceProducerResponse {
    throttle_time_ms: u32,
    result: NormalizedFenceProducerResult,
    retained_bytes: usize,
}

impl NormalizedFenceProducerResponse {
    pub(crate) const fn into_parts(self) -> (u32, NormalizedFenceProducerResult, usize) {
        (self.throttle_time_ms, self.result, self.retained_bytes)
    }
}

/// Malformed or over-budget response facts before deterministic settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FenceProducerResponseFailure {
    NegativeThrottleTime,
    InvalidIdentity,
    RetainedBytes { required: usize, limit: usize },
}

/// Original-deadline expiry before one generated fencing request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FenceProducerDeadlineElapsed;

/// Derives the positive broker transaction timeout from the original deadline.
pub(crate) fn remaining_fence_producer_timeout_ms(
    now: Moment,
    deadline: Deadline,
) -> Result<u32, FenceProducerDeadlineElapsed> {
    let remaining = remaining_timeout_ms(now, deadline)
        .map_err(|RequestDeadlineError::DeadlineElapsed| FenceProducerDeadlineElapsed)?;
    u32::try_from(remaining).map_err(|_| FenceProducerDeadlineElapsed)
}

/// Normalizes API key 22 without adding routing, retry, or fencing policy.
pub(crate) fn normalize_fence_producer_response(
    response: &InitProducerIdResponse,
    transactional_id: &str,
    retained_limit: usize,
) -> Result<NormalizedFenceProducerResponse, FenceProducerResponseFailure> {
    let throttle_time_ms = u32::try_from(response.throttle_time_ms)
        .map_err(|_| FenceProducerResponseFailure::NegativeThrottleTime)?;
    let retained_bytes = transactional_id.len();
    if retained_bytes > retained_limit {
        return Err(FenceProducerResponseFailure::RetainedBytes {
            required: retained_bytes,
            limit: retained_limit,
        });
    }
    let result = match normalize_transaction_init_response(response) {
        Ok(identity) => NormalizedFenceProducerResult::Fenced {
            producer_id: identity.producer_id,
            producer_epoch: identity.producer_epoch,
        },
        Err(TransactionInitResponseFailure::Broker { code, .. }) => {
            NormalizedFenceProducerResult::BrokerFailed { code }
        }
        Err(TransactionInitResponseFailure::InvalidIdentity) => {
            return Err(FenceProducerResponseFailure::InvalidIdentity);
        }
    };
    Ok(NormalizedFenceProducerResponse {
        throttle_time_ms,
        result,
        retained_bytes,
    })
}
