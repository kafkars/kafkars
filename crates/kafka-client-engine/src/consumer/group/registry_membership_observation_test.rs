//! Rediscovery unsettled-accounting and hidden-deadline scenarios.

use super::{
    classic_group_rejoin_test_support::{arm_rejoin, entry_mut},
    registry_test_support::{register, started_registry, stop_registry},
};

#[test]
fn rediscovery_counts_separately_and_hides_its_rejoin_deadline() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let schedule = arm_rejoin(&mut registry, group_id, 10);
    entry_mut(&mut registry, group_id)
        .rediscovery
        .prepare_rediscovery_install()
        .unwrap_or_else(|error| panic!("rediscovery install failed: {error:?}"))
        .commit();

    assert_eq!(registry.membership_unsettled(), 2);
    assert_eq!(registry.membership_next_deadline(), None);

    entry_mut(&mut registry, group_id)
        .rediscovery
        .clear_rediscovery_after_driver_shutdown();
    assert_eq!(registry.membership_unsettled(), 1);
    assert_eq!(registry.membership_next_deadline(), Some(schedule.due()));
    stop_registry(&mut registry);
}
