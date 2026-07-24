//! Shared original-deadline to broker-timeout scenarios.

use super::timeout::{AdminRequestDeadlineError, remaining_timeout_ms};

#[test]
fn timeout_ceil_never_restarts_or_extends_the_original_deadline() {
    assert_eq!(
        remaining_timeout_ms(
            kafka_client_core::Moment::from_tick(1_000_001),
            kafka_client_core::Deadline::from_tick(2_000_000),
        ),
        Ok(1)
    );
    assert_eq!(
        remaining_timeout_ms(
            kafka_client_core::Moment::from_tick(2_000_000),
            kafka_client_core::Deadline::from_tick(2_000_000),
        ),
        Err(AdminRequestDeadlineError::DeadlineElapsed)
    );
}
