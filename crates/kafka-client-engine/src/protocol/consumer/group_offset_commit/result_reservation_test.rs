//! Pre-admission result-capacity bounds and allocation-failure scenarios.

use super::{
    result_reservation::{
        GroupOffsetCommitResultReservation, GroupOffsetCommitResultReservationError,
    },
    validation::MAX_GROUP_OFFSET_COMMIT_ENTRIES,
};

#[test]
fn exact_result_capacity_is_reserved_before_preparation() {
    let reservation = GroupOffsetCommitResultReservation::try_new(3)
        .unwrap_or_else(|error| panic!("reserve result capacity: {error:?}"));
    assert_eq!(reservation.entry_count(), 3);
    assert!(reservation.outcomes_capacity() >= 3);
    assert!(!reservation.outcomes_ptr_for_test().is_null());
}

#[test]
fn reservation_rejects_unbounded_and_unallocatable_capacity() {
    assert!(matches!(
        GroupOffsetCommitResultReservation::try_new(MAX_GROUP_OFFSET_COMMIT_ENTRIES + 1),
        Err(GroupOffsetCommitResultReservationError::EntryCapacity {
            actual,
            limit: MAX_GROUP_OFFSET_COMMIT_ENTRIES,
        }) if actual == MAX_GROUP_OFFSET_COMMIT_ENTRIES + 1
    ));
    assert!(matches!(
        GroupOffsetCommitResultReservation::try_new_with_capacity_for_test(2, 1),
        Err(
            GroupOffsetCommitResultReservationError::ReservationCapacity {
                required: 2,
                actual: 1,
            }
        )
    ));
    assert!(matches!(
        GroupOffsetCommitResultReservation::try_new_with_capacity_for_test(1, usize::MAX),
        Err(GroupOffsetCommitResultReservationError::AllocationFailed)
    ));
}
