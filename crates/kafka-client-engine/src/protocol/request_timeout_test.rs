//! Cross-domain evidence for the one absolute-deadline timeout mechanism.

use std::time::Instant;

use crate::clock::OperationDeadline;

use super::request_timeout::{RequestDeadlineError, remaining_timeout_ms};

#[test]
fn millisecond_ceiling_keeps_the_original_transport_instant_unchanged() {
    let transport = Instant::now();
    let deadline = OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(2_000_000),
        transport,
    );
    assert_eq!(
        remaining_timeout_ms(
            kafka_client_core::Moment::from_tick(1_000_001),
            deadline.core(),
        ),
        Ok(1)
    );
    assert_eq!(deadline.transport(), transport);
    assert_eq!(
        remaining_timeout_ms(
            kafka_client_core::Moment::from_tick(2_000_000),
            deadline.core(),
        ),
        Err(RequestDeadlineError::DeadlineElapsed)
    );
}

#[test]
fn timeout_saturates_only_the_generated_integer_field() {
    assert_eq!(
        remaining_timeout_ms(
            kafka_client_core::Moment::from_tick(0),
            kafka_client_core::Deadline::from_tick(u64::MAX),
        ),
        Ok(i32::MAX)
    );
}
