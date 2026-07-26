//! Post-driver rediscovery disposal and gate-clearing scenarios.

use super::{
    classic_group_rediscovery_execution_test::invalidating_registry,
    registry_test_support::stop_registry,
};

#[test]
fn post_driver_recovery_discards_the_registry_owner_then_reopens_the_gate() {
    let (mut registry, group_id) = invalidating_registry();

    registry
        .recover_classic_coordinator_invalidations_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("rediscovery recovery failed: {error:?}"));

    assert!(registry.coordinator_invalidations.is_none());
    assert!(
        registry
            .coordinator_invalidation_shutdown_recovery
            .is_none()
    );
    assert!(
        !registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"))
            .rediscovery
            .blocks_join()
    );
    stop_registry(&mut registry);
}
