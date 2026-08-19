//! `InitProducerId` normalization scenarios for caller-selected producer fencing.

use kafka_client_core::{Deadline, Moment};
use kafka_wire::InitProducerIdResponse;

use super::fence_producers::FenceProducerDeadlineElapsed;
use super::{
    FenceProducerResponseFailure, NormalizedFenceProducerResult, normalize_fence_producer_response,
    remaining_fence_producer_timeout_ms,
};

#[test]
fn transaction_timeout_is_positive_and_uses_the_original_deadline() {
    assert_eq!(
        remaining_fence_producer_timeout_ms(
            Moment::from_tick(1_000_001),
            Deadline::from_tick(2_000_000),
        ),
        Ok(1)
    );
    assert_eq!(
        remaining_fence_producer_timeout_ms(
            Moment::from_tick(2_000_000),
            Deadline::from_tick(2_000_000),
        ),
        Err(FenceProducerDeadlineElapsed)
    );
}

#[test]
fn identity_exact_error_and_nonnegative_throttle_cross_losslessly() {
    let normalized =
        normalize_fence_producer_response(&response(0, 91, 7, 41), "invoice-writer", 128)
            .unwrap_or_else(|error| panic!("valid fencing response: {error:?}"));
    let (throttle_time_ms, result, retained_bytes) = normalized.into_parts();
    assert_eq!(throttle_time_ms, 41);
    assert_eq!(retained_bytes, "invoice-writer".len());
    assert_eq!(
        result,
        NormalizedFenceProducerResult::Fenced {
            producer_id: 91,
            producer_epoch: 7,
        }
    );

    let normalized =
        normalize_fence_producer_response(&response(-31_777, -1, -1, 0), "audit-writer", 128)
            .unwrap_or_else(|error| panic!("broker rejection remains an outcome: {error:?}"));
    let (_, result, retained_bytes) = normalized.into_parts();
    assert_eq!(retained_bytes, "audit-writer".len());
    let NormalizedFenceProducerResult::BrokerFailed { code } = result else {
        panic!("exact broker error expected");
    };
    assert_eq!(code.get(), -31_777);
}

#[test]
fn malformed_and_over_budget_responses_remain_distinct() {
    assert_eq!(
        normalize_fence_producer_response(&response(0, 91, 7, -1), "invoice-writer", 128),
        Err(FenceProducerResponseFailure::NegativeThrottleTime)
    );
    assert_eq!(
        normalize_fence_producer_response(&response(0, -1, 7, 0), "invoice-writer", 128),
        Err(FenceProducerResponseFailure::InvalidIdentity)
    );
    assert_eq!(
        normalize_fence_producer_response(&response(0, 91, 7, 0), "invoice-writer", 1),
        Err(FenceProducerResponseFailure::RetainedBytes {
            required: "invoice-writer".len(),
            limit: 1,
        })
    );
}

fn response(
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
    throttle_time_ms: i32,
) -> InitProducerIdResponse {
    let mut response = InitProducerIdResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.error_code = error_code;
    response.producer_id = producer_id;
    response.producer_epoch = producer_epoch;
    response
}
