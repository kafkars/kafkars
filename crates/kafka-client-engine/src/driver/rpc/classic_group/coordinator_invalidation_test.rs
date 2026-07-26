//! Bounded coordinator-invalidation reservation scenarios.

use kafka_client_core::GroupId;

use super::coordinator_invalidation::{
    ClassicCoordinatorInvalidationReserveError, ClassicCoordinatorInvalidations,
};

#[test]
fn reservation_is_capacity_bounded_without_mutating_before_install() {
    let group_id = group(1);
    let mut invalidations = ClassicCoordinatorInvalidations::new(1);
    let permit = invalidations
        .try_reserve(group_id)
        .unwrap_or_else(|error| panic!("test reservation failed: {error:?}"));

    assert_eq!(permit.group_id(), group_id);
    drop(permit);
    assert_eq!(invalidations.retained_count(), 0);
    assert!(!invalidations.blocks_join(group_id));
}

#[test]
fn zero_capacity_rejects_without_retaining_a_group_gate() {
    let group_id = group(1);
    let mut invalidations = ClassicCoordinatorInvalidations::new(0);

    assert_eq!(
        invalidations.try_reserve(group_id).err(),
        Some(ClassicCoordinatorInvalidationReserveError::Capacity { limit: 0 })
    );
    assert_eq!(invalidations.retained_count(), 0);
    assert!(!invalidations.blocks_join(group_id));
}

fn group(value: u64) -> GroupId {
    GroupId::try_from_raw(value).unwrap_or_else(|| panic!("test group must be nonzero"))
}
