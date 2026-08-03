//! Membership unsettled-accounting and hidden-deadline scenarios.

use super::{
    classic_group_rejoin_test_support::{arm_rejoin, entry_mut},
    registry_event_reconciliation_test::{
        defer_rejoin_during_reconciliation, prepared_reconciliation,
    },
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

#[test]
fn prepared_classic_reconciliation_counts_as_one_membership_owner() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let original = core::mem::replace(entry, prepared_reconciliation());

    assert_eq!(registry.membership_unsettled(), 1);

    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("prepared group"));
    let _prepared = core::mem::replace(entry, original);
    assert_eq!(registry.membership_unsettled(), 0);
    stop_registry(&mut registry);
}

#[test]
fn prepared_classic_reconciliation_hides_its_deferred_rejoin_deadline() {
    let mut pending = prepared_reconciliation();
    let schedule = defer_rejoin_during_reconciliation(&mut pending);

    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let entry = entry_mut(&mut registry, group_id);
    let original = core::mem::replace(entry, pending);

    assert_eq!(
        entry_mut(&mut registry, group_id).rejoin.next_deadline(),
        Some(schedule.due())
    );
    assert_eq!(registry.membership_next_deadline(), None);

    let entry = entry_mut(&mut registry, group_id);
    let _pending = core::mem::replace(entry, original);
    stop_registry(&mut registry);
}
