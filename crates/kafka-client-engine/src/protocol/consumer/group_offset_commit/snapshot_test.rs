//! Prepared snapshot transfer and result-reservation mismatch scenarios.

use std::sync::Arc;

use super::{
    model::PreparedGroupOffsetCommit,
    model_test::{entry, inputs, reservation, topic},
    preparation::GroupOffsetCommitPreparationErrorKind,
};

#[test]
fn reservation_mismatch_returns_the_exact_preallocated_owner() {
    let (effect, deadline, session, topics) = inputs(
        vec![entry(1, 0, 10, None)],
        4,
        Arc::from("readers"),
        Arc::from("member-a"),
        vec![topic(1, Arc::from("orders"))],
    );
    let result_reservation = reservation(2);
    let expected_pointer = result_reservation.outcomes_ptr_for_test();
    let expected_capacity = result_reservation.outcomes_capacity();
    let error = match PreparedGroupOffsetCommit::from_effect(
        effect,
        deadline,
        session,
        topics,
        result_reservation,
    ) {
        Ok(_) => panic!("mismatched result reservation must be rejected"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        GroupOffsetCommitPreparationErrorKind::ResultReservationMismatch {
            entries: 1,
            reserved: 2,
        }
    );
    let (_effect, _deadline, _session, _topics, result_reservation) = error.into_parts();
    assert_eq!(result_reservation.outcomes_ptr_for_test(), expected_pointer);
    assert_eq!(result_reservation.outcomes_capacity(), expected_capacity);
}
