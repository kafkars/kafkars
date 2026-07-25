//! Join settlement fencing, restoration, and recovery scenarios.

use kafka_driver::{ApiVersion, CompletionError};
use kafka_wire::JoinGroupResponse;

use super::{
    join_group_calls::TrackedJoinGroupCalls,
    join_group_settlement::{JoinGroupBeginError, JoinGroupPoll},
    join_group_terminal::retain_join_group_terminal,
    join_group_terminal_test::{deadline, key, key_with_deadline},
};

#[test]
fn same_cycle_and_deadline_from_another_group_cannot_take_settlement() {
    let deadline = deadline();
    let first = key_with_deadline(1, deadline);
    let second = key_with_deadline(2, deadline);
    let mut calls = terminal_calls(first);

    assert_eq!(
        calls.begin_join_group_settlement(second).err(),
        Some(JoinGroupBeginError::KeyMismatch {
            settled: first,
            supplied: second,
        })
    );
    assert_eq!(
        calls.poll_join_group(),
        Ok(JoinGroupPoll::TerminalReady { key: first })
    );
}

#[test]
fn failed_interpretation_can_restore_the_exact_raw_terminal() {
    let key = key(1);
    let mut calls = terminal_calls(key);
    let terminal = calls
        .begin_join_group_settlement(key)
        .unwrap_or_else(|error| panic!("test settlement must begin: {error:?}"));

    calls
        .restore_join_group_settlement(terminal)
        .unwrap_or_else(|_failure| panic!("exact terminal must restore"));

    let terminal = calls
        .begin_join_group_settlement(key)
        .unwrap_or_else(|error| panic!("restored settlement must begin: {error:?}"));
    assert_eq!(terminal.selected_version(), Some(3));
    calls
        .confirm_join_group_settlement(key)
        .unwrap_or_else(|error| panic!("exact route confirmation must finish: {error:?}"));
    assert_eq!(calls.retained_join_group_count(), 0);
}

#[test]
fn shutdown_recovery_consumes_registry_and_reuses_preallocated_active_storage() {
    let mut calls = TrackedJoinGroupCalls::new(3);
    let allocated_capacity = calls.calls.capacity();
    let key = key(1);
    calls.install_completion_failure_for_test(key, CompletionError::Closed);

    let mut recovery = calls.recover_join_groups_after_driver_shutdown();

    assert!(allocated_capacity >= 3);
    assert_eq!(
        recovery.active_storage_capacity_for_test(),
        allocated_capacity
    );
    let failure = recovery
        .take_completion()
        .unwrap_or_else(|| panic!("completion ownership must recover"));
    assert_eq!(failure.into_parts(), (key, CompletionError::Closed));
    assert!(recovery.is_empty());
}

#[test]
fn shutdown_recovery_preserves_settled_and_external_pending_ownership() {
    let settled_key = key(1);
    let settled_calls = terminal_calls(settled_key);
    let mut settled_recovery = settled_calls.recover_join_groups_after_driver_shutdown();
    let settled = settled_recovery
        .take_settled()
        .unwrap_or_else(|| panic!("raw settled terminal must recover"));
    assert_eq!(settled.key(), settled_key);
    assert_eq!(settled.selected_version(), Some(3));
    assert!(settled_recovery.is_empty());

    let pending_key = key(2);
    let mut pending_calls = terminal_calls(pending_key);
    let external = pending_calls
        .begin_join_group_settlement(pending_key)
        .unwrap_or_else(|error| panic!("test settlement must begin: {error:?}"));
    let mut pending_recovery = pending_calls.recover_join_groups_after_driver_shutdown();
    let pending = pending_recovery
        .take_pending()
        .unwrap_or_else(|| panic!("pending route owner must recover"));
    assert_eq!(pending.key(), external.key());
    assert_eq!(external.selected_version(), Some(3));
    assert!(pending_recovery.is_empty());
}

fn terminal_calls(key: super::join_group_terminal::JoinGroupCallKey) -> TrackedJoinGroupCalls {
    let mut calls = TrackedJoinGroupCalls::new(1);
    let terminal = retain_join_group_terminal(
        key,
        Some(ApiVersion::new(3)),
        Ok(JoinGroupResponse::default()),
    );
    calls.install_terminal_for_test(
        terminal.key(),
        terminal.selected_version(),
        terminal.into_parts().2,
    );
    calls
}
