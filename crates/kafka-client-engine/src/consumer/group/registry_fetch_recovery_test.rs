//! Registry-level post-driver Fetch release and terminal entry retirement.

use super::registry_test_support::{install_session, register, started_registry};

#[test]
fn successful_post_driver_recovery_consumes_terminal_entries_into_reserved_fetch_reports() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "group-a");
    install_session(&mut registry, group_id);

    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("group registry recovery: {error}"));

    assert_eq!(registry.registered_group_count(), 0);
    assert_eq!(registry.retained_group_bytes(), 0);
    let recovery = registry
        .fetch_shutdown_recovery(group_id)
        .unwrap_or_else(|| panic!("group Fetch recovery report expected"));
    assert_eq!(recovery.activation(), None);
    assert_eq!(recovery.machine_assignment(), None);
    assert_eq!(recovery.effects(), 0);
    assert_eq!(recovery.prepared(), 0);
    assert_eq!(recovery.fetch_retained(), (0, 0, 0));

    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish recovered registry: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group notifier join: {error}"));
}
