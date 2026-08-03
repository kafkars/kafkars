//! Registry-level post-driver Fetch release and accepted-close terminal retirement.

use super::{
    classic_group_leave::{
        GroupConsumerCloseCompletionObservation, GroupConsumerCloseTerminal,
        GroupConsumerCloseTerminalFailureKind,
    },
    registry_close::GroupConsumerRemovalError,
    registry_test_support::{deadline, install_session, register, started_registry},
};

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

#[test]
fn post_driver_recovery_publishes_the_accepted_close_terminal_before_entry_drop() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "group-a");
    install_session(&mut registry, group_id);
    let authority = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered group"))
        .close_authority();
    let completion = registry
        .close_group_explicit(group_id, deadline(100), &authority)
        .unwrap_or_else(|error| panic!("accepted close: {error:?}"));

    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("group registry recovery: {error}"));

    assert!(matches!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Terminal(GroupConsumerCloseTerminal::Failed(
            failure
        )) if failure.kind == GroupConsumerCloseTerminalFailureKind::DriverShutdown
    ));
    assert_eq!(registry.registered_group_count(), 0);
    assert_eq!(registry.retained_group_bytes(), 0);
    assert!(registry.fetch_shutdown_recovery(group_id).is_some());

    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish recovered registry: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group notifier join: {error}"));
}

#[test]
fn duplicate_recovery_publication_is_an_explicit_terminal_invariant_error() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "group-a");
    let later_group_id = register(&mut registry, "group-b");
    let authority = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered group"))
        .close_authority();
    let completion = registry
        .close_group_explicit(group_id, deadline(100), &authority)
        .unwrap_or_else(|error| panic!("accepted close: {error:?}"));
    let later_authority = registry
        .entry(later_group_id)
        .unwrap_or_else(|| panic!("later registered group"))
        .close_authority();
    let later_completion = registry
        .close_group_explicit(later_group_id, deadline(100), &later_authority)
        .unwrap_or_else(|error| panic!("later accepted close: {error:?}"));
    registry.close_admission();
    registry.recover_classic_group_leaves_after_driver_shutdown();
    assert!(
        registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .unwrap_or_else(|| panic!("closing group"))
            .leave
            .publish_terminal()
    );

    assert_eq!(
        registry.recover_fetch_after_driver_shutdown(),
        Err(GroupConsumerRemovalError::TerminalInvariant)
    );
    assert!(matches!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Terminal(GroupConsumerCloseTerminal::Failed(
            failure
        )) if failure.kind == GroupConsumerCloseTerminalFailureKind::DriverShutdown
    ));
    assert!(matches!(
        later_completion.observe(),
        GroupConsumerCloseCompletionObservation::Terminal(GroupConsumerCloseTerminal::Failed(
            failure
        )) if failure.kind == GroupConsumerCloseTerminalFailureKind::DriverShutdown
    ));
    assert_eq!(registry.registered_group_count(), 0);
    assert!(registry.fetch_shutdown_recovery(group_id).is_some());
    assert!(registry.fetch_shutdown_recovery(later_group_id).is_some());

    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish invariant registry: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group notifier join: {error}"));
}
