//! Prepared snapshot transfer and reservation-mismatch scenarios.

use std::sync::Arc;

use super::{
    model::PreparedGroupOffsetCommit,
    model_test::{entry, entry_reservation, inputs, reservation, topic},
    preparation::GroupOffsetCommitPreparationErrorKind,
};

#[test]
fn result_reservation_mismatch_returns_both_exact_preallocated_owners() {
    let (effect, deadline, session, topics) = inputs(
        vec![entry(1, 0, 10, None)],
        4,
        Arc::from("readers"),
        Arc::from("member-a"),
        vec![topic(1, Arc::from("orders"))],
    );
    let entry_reservation = entry_reservation(1);
    let expected_entry_pointer = entry_reservation.entries_ptr_for_test();
    let expected_entry_capacity = entry_reservation.entries_capacity();
    let result_reservation = reservation(2);
    let expected_pointer = result_reservation.outcomes_ptr_for_test();
    let expected_capacity = result_reservation.outcomes_capacity();
    let error = match PreparedGroupOffsetCommit::from_effect(
        effect,
        deadline,
        session,
        topics,
        entry_reservation,
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
    let (_effect, _deadline, _session, _topics, entry_reservation, result_reservation) =
        error.into_parts();
    assert_eq!(
        entry_reservation.entries_ptr_for_test(),
        expected_entry_pointer
    );
    assert_eq!(
        entry_reservation.entries_capacity(),
        expected_entry_capacity
    );
    assert_eq!(result_reservation.outcomes_ptr_for_test(), expected_pointer);
    assert_eq!(result_reservation.outcomes_capacity(), expected_capacity);
}

#[test]
fn entry_reservation_mismatch_returns_both_exact_preallocated_owners() {
    let (effect, deadline, session, topics) = inputs(
        vec![entry(1, 0, 10, None)],
        4,
        Arc::from("readers"),
        Arc::from("member-a"),
        vec![topic(1, Arc::from("orders"))],
    );
    let entry_reservation = entry_reservation(2);
    let expected_entry_pointer = entry_reservation.entries_ptr_for_test();
    let expected_entry_capacity = entry_reservation.entries_capacity();
    let result_reservation = reservation(1);
    let expected_result_pointer = result_reservation.outcomes_ptr_for_test();
    let expected_result_capacity = result_reservation.outcomes_capacity();
    let error = match PreparedGroupOffsetCommit::from_effect(
        effect,
        deadline,
        session,
        topics,
        entry_reservation,
        result_reservation,
    ) {
        Ok(_) => panic!("mismatched entry reservation must be rejected"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        GroupOffsetCommitPreparationErrorKind::EntryReservationMismatch {
            entries: 1,
            reserved: 2,
        }
    );
    let (_effect, _deadline, _session, _topics, entry_reservation, result_reservation) =
        error.into_parts();
    assert_eq!(
        entry_reservation.entries_ptr_for_test(),
        expected_entry_pointer
    );
    assert_eq!(
        entry_reservation.entries_capacity(),
        expected_entry_capacity
    );
    assert_eq!(
        result_reservation.outcomes_ptr_for_test(),
        expected_result_pointer
    );
    assert_eq!(
        result_reservation.outcomes_capacity(),
        expected_result_capacity
    );
}
