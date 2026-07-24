//! Position preparation errors preserve the original deadline pair.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;

use super::{
    position_execution::PreparedPositionResolution, position_execution_test::assignment,
    position_prepare_error::PreparePositionError,
};
use crate::{clock::OperationDeadline, protocol::consumer::ListOffsetsIsolation};

#[test]
fn deadline_pair_mismatch_is_an_invariant_not_a_core_attempt_failure() {
    let (effect, _) = assignment(&[3], Deadline::from_tick(20));
    let error = PreparedPositionResolution::new(
        effect[0],
        "orders".to_owned(),
        ListOffsetsIsolation::ReadUncommitted,
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(21),
            Instant::now() + Duration::from_secs(1),
        ),
    )
    .err()
    .unwrap_or_else(|| panic!("deadline mismatch must fail preparation"));

    assert_eq!(
        error,
        PreparePositionError::DeadlineMismatch {
            effect: Deadline::from_tick(20),
            operation: Deadline::from_tick(21),
        }
    );
}
