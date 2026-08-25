//! Transaction lifecycle scalar and terminal shape evidence.

use super::{
    TransactionEndMode, TransactionLifecycleTerminal, TransactionSendId, TransactionSequenceLease,
};

#[test]
fn lifecycle_models_keep_commit_abort_and_success_terminals_distinct() {
    assert_ne!(TransactionEndMode::Commit, TransactionEndMode::Abort);
    assert_ne!(
        TransactionLifecycleTerminal::Committed,
        TransactionLifecycleTerminal::Aborted
    );
    assert_ne!(
        TransactionLifecycleTerminal::Aborted,
        TransactionLifecycleTerminal::Fatal
    );
}

#[test]
fn accepted_send_identity_preserves_the_engine_scalar() {
    let send_id = TransactionSendId::from_raw(u64::MAX);

    assert_eq!(send_id.get(), u64::MAX);
}

#[test]
fn transaction_sequence_lease_rejects_empty_or_negative_ranges() {
    assert_eq!(TransactionSequenceLease::try_new(-1, 1), None);
    assert_eq!(TransactionSequenceLease::try_new(0, 0), None);
    let lease = TransactionSequenceLease::try_new(17, 3)
        .unwrap_or_else(|| panic!("nonempty nonnegative range is valid"));
    assert_eq!(lease.base_sequence(), 17);
    assert_eq!(lease.record_count(), 3);
}
