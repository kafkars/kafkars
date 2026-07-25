//! Pre-admission prepared-entry capacity and allocation-failure scenarios.

use std::mem::size_of;

use super::{
    entry_reservation::{
        GroupOffsetCommitEntryReservation, GroupOffsetCommitEntryReservationError,
    },
    model::PreparedGroupOffsetCommitEntry,
    validation::MAX_GROUP_OFFSET_COMMIT_ENTRIES,
};

#[test]
fn prepared_entry_capacity_is_reserved_before_preparation() {
    let reservation = GroupOffsetCommitEntryReservation::try_new(3)
        .unwrap_or_else(|error| panic!("reserve prepared entry capacity: {error:?}"));
    assert_eq!(reservation.entry_count(), 3);
    assert!(reservation.entries_capacity() >= 3);
    assert_eq!(
        reservation.reserved_bytes(),
        reservation
            .entries_capacity()
            .checked_mul(size_of::<PreparedGroupOffsetCommitEntry>())
    );
    assert!(!reservation.entries_ptr_for_test().is_null());
}

#[test]
fn reservation_rejects_unbounded_and_unallocatable_capacity() {
    assert!(matches!(
        GroupOffsetCommitEntryReservation::try_new(MAX_GROUP_OFFSET_COMMIT_ENTRIES + 1),
        Err(GroupOffsetCommitEntryReservationError::EntryCapacity {
            actual,
            limit: MAX_GROUP_OFFSET_COMMIT_ENTRIES,
        }) if actual == MAX_GROUP_OFFSET_COMMIT_ENTRIES + 1
    ));
    assert!(matches!(
        GroupOffsetCommitEntryReservation::try_new_with_capacity_for_test(2, 1),
        Err(
            GroupOffsetCommitEntryReservationError::ReservationCapacity {
                required: 2,
                actual: 1,
            }
        )
    ));
    assert!(matches!(
        GroupOffsetCommitEntryReservation::try_new_with_capacity_for_test(1, usize::MAX),
        Err(GroupOffsetCommitEntryReservationError::AllocationFailed)
    ));
}
