//! Exact and lossless Sync receipt reconciliation after driver shutdown.

use kafka_driver::CompletionError;

use super::{
    sync_group_calls::{AcceptedSyncGroupCall, TrackedSyncGroupCalls},
    sync_group_reconciliation::{
        RecoveredSyncGroupOwnership, SyncGroupShutdownReconciliationError,
    },
    sync_group_terminal_test::{deadline, key, key_with_deadline},
};

#[test]
fn every_recovered_sync_state_consumes_the_exact_receipt() {
    let exact = key(1);
    for recovered in recovered_states(exact) {
        recovered
            .reconcile_sync_group_after_driver_shutdown(accepted(exact))
            .unwrap_or_else(|_failure| panic!("exact recovered Sync owner must reconcile"));
    }
}

#[test]
fn cross_group_mismatch_returns_both_sync_owners_unchanged() {
    let first = key(1);
    let second = key(2);
    let failure = RecoveredSyncGroupOwnership::active_for_test(first)
        .reconcile_sync_group_after_driver_shutdown(accepted(second))
        .err()
        .unwrap_or_else(|| panic!("cross-group receipt must reject"));
    let (second_receipt, first_recovered, error) = failure.into_parts();

    assert_eq!(
        error,
        SyncGroupShutdownReconciliationError::KeyMismatch {
            recovered: first,
            supplied: second,
        }
    );
    assert_eq!(first_recovered.key(), first);
    assert_eq!(second_receipt.key(), second);
    first_recovered
        .reconcile_sync_group_after_driver_shutdown(accepted(first))
        .unwrap_or_else(|_failure| panic!("returned recovered owner must remain intact"));
    RecoveredSyncGroupOwnership::active_for_test(second)
        .reconcile_sync_group_after_driver_shutdown(second_receipt)
        .unwrap_or_else(|_failure| panic!("returned receipt must remain intact"));
}

#[test]
fn changed_deadline_mismatch_returns_both_sync_owners_unchanged() {
    let original_deadline = deadline();
    let recovered_key = key_with_deadline(1, original_deadline);
    let changed_deadline = crate::clock::OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(original_deadline.core().tick() + 1),
        original_deadline.transport(),
    );
    let supplied_key = key_with_deadline(1, changed_deadline);
    let failure = RecoveredSyncGroupOwnership::active_for_test(recovered_key)
        .reconcile_sync_group_after_driver_shutdown(accepted(supplied_key))
        .err()
        .unwrap_or_else(|| panic!("changed-deadline receipt must reject"));
    let (supplied, recovered, error) = failure.into_parts();

    assert_eq!(
        error,
        SyncGroupShutdownReconciliationError::KeyMismatch {
            recovered: recovered_key,
            supplied: supplied_key,
        }
    );
    recovered
        .reconcile_sync_group_after_driver_shutdown(accepted(recovered_key))
        .unwrap_or_else(|_failure| panic!("returned recovered owner must remain intact"));
    RecoveredSyncGroupOwnership::active_for_test(supplied_key)
        .reconcile_sync_group_after_driver_shutdown(supplied)
        .unwrap_or_else(|_failure| panic!("returned receipt must remain intact"));
}

fn recovered_states(
    key: super::sync_group_terminal::SyncGroupCallKey,
) -> [RecoveredSyncGroupOwnership; 4] {
    let active = RecoveredSyncGroupOwnership::active_for_test(key);

    let settled_calls = terminal_calls(key);
    let settled = settled_calls
        .recover_sync_groups_after_driver_shutdown()
        .take_settled()
        .unwrap_or_else(|| panic!("settled owner must recover"));

    let mut pending_calls = terminal_calls(key);
    let receipt = accepted(key);
    let _external = pending_calls
        .begin_sync_group_settlement(&receipt)
        .unwrap_or_else(|error| panic!("pending settlement must begin: {error:?}"));
    let pending = pending_calls
        .recover_sync_groups_after_driver_shutdown()
        .take_pending()
        .unwrap_or_else(|| panic!("pending owner must recover"));
    drop(receipt);

    let mut completion_calls = TrackedSyncGroupCalls::new(1);
    completion_calls.install_completion_failure_for_test(key, CompletionError::Closed);
    let completion = completion_calls
        .recover_sync_groups_after_driver_shutdown()
        .take_completion()
        .unwrap_or_else(|| panic!("completion owner must recover"));

    [active, settled, pending, completion]
}

fn accepted(key: super::sync_group_terminal::SyncGroupCallKey) -> AcceptedSyncGroupCall {
    AcceptedSyncGroupCall::from_key_for_test(key)
}

fn terminal_calls(key: super::sync_group_terminal::SyncGroupCallKey) -> TrackedSyncGroupCalls {
    let mut calls = TrackedSyncGroupCalls::new(1);
    calls.install_terminal_for_test(key, Some(2), Ok(kafka_wire::SyncGroupResponse::default()));
    calls
}
