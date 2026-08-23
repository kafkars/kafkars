//! Share-coordinator invalidation capacity and duplicate fencing scenarios.

use kafka_client_core::GroupId;

use super::invalidation::{
    ShareCoordinatorInvalidationReserveError, ShareCoordinatorInvalidations,
};

#[test]
fn reservation_is_bounded_and_does_not_mutate_before_install() {
    let group_id = group(1);
    let mut invalidations = ShareCoordinatorInvalidations::try_new(1)
        .unwrap_or_else(|error| panic!("reservation: {error:?}"));
    let permit = invalidations
        .try_reserve(group_id)
        .unwrap_or_else(|error| panic!("permit: {error:?}"));
    drop(permit);
    assert_eq!(invalidations.retained_count(), 0);
    assert!(!invalidations.blocks_submission(group_id));
}

#[test]
fn zero_capacity_rejects_without_retaining_a_gate() {
    let group_id = group(1);
    let mut invalidations = ShareCoordinatorInvalidations::try_new(0)
        .unwrap_or_else(|error| panic!("reservation: {error:?}"));
    assert_eq!(
        invalidations.try_reserve(group_id).err(),
        Some(ShareCoordinatorInvalidationReserveError::Capacity { limit: 0 })
    );
    assert_eq!(invalidations.retained_count(), 0);
}

fn group(value: u64) -> GroupId {
    GroupId::try_from_raw(value).unwrap_or_else(|| panic!("nonzero test group"))
}
