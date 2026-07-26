//! Post-driver-shutdown coordinator-invalidation recovery scenarios.

use kafka_client_core::GroupId;
use kafka_driver::CompletionError;

use super::coordinator_invalidation::{
    ClassicCoordinatorInvalidationState, ClassicCoordinatorInvalidations,
};

#[test]
fn shutdown_recovery_reuses_storage_and_explicitly_discards_a_retained_fault() {
    let mut invalidations = ClassicCoordinatorInvalidations::new(3);
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("test group must be nonzero"));
    invalidations
        .entries
        .push(ClassicCoordinatorInvalidationState::CompletionFailed {
            group_id,
            source: CompletionError::Closed,
        });
    let capacity = invalidations.entries.capacity();
    let mut recovery = invalidations.recover_after_driver_shutdown();

    assert!(capacity >= 3);
    assert_eq!(recovery.storage_capacity_for_test(), capacity);
    assert_eq!(recovery.retained_count(), 1);
    assert_eq!(recovery.discard_one_after_driver_shutdown(), Some(group_id));
    assert!(recovery.is_empty());
}
