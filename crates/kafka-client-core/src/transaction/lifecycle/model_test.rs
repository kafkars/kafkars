//! Transaction lifecycle scalar and terminal shape evidence.

use super::{TransactionEndMode, TransactionEndOutcome, TransactionLifecycleTerminal};

#[test]
fn lifecycle_models_keep_commit_abort_success_and_fatal_distinct() {
    assert_ne!(TransactionEndMode::Commit, TransactionEndMode::Abort);
    assert_ne!(
        TransactionEndOutcome::Succeeded,
        TransactionEndOutcome::Fatal
    );
    assert_ne!(
        TransactionLifecycleTerminal::Committed,
        TransactionLifecycleTerminal::Aborted
    );
    assert_ne!(
        TransactionLifecycleTerminal::Aborted,
        TransactionLifecycleTerminal::Fatal
    );
}
