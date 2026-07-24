//! Admin-local validation layered over the shared deadline mechanism.

use super::request_timeout_error::{AdminRequestDeadlineError, remaining_timeout_ms};

#[test]
fn shared_deadline_elapsed_fact_enters_the_admin_error_domain() {
    assert_eq!(
        remaining_timeout_ms(
            kafka_client_core::Moment::from_tick(10),
            kafka_client_core::Deadline::from_tick(10),
        ),
        Err(AdminRequestDeadlineError::DeadlineElapsed)
    );
}
