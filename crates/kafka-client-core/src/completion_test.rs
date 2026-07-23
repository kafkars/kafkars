//! Tests for terminal-completion capacity.

use crate::{CompletionLedger, CompletionLedgerError, OperationId};

#[test]
fn completion_capacity_is_reserved_before_admission() {
    let mut ledger = CompletionLedger::new(1);
    let first = OperationId::from_raw(1);
    let second = OperationId::from_raw(2);

    assert_eq!(ledger.reserve(first), Ok(()));
    assert_eq!(ledger.reserve(second), Err(CompletionLedgerError::Full));
    assert_eq!(ledger.mark_terminal(first), Ok(()));
    assert_eq!(ledger.reclaim(first), Ok(()));
    assert!(ledger.is_empty());
}

#[test]
fn terminal_completion_is_exactly_once() {
    let mut ledger = CompletionLedger::new(2);
    let id = OperationId::from_raw(3);

    assert_eq!(ledger.reserve(id), Ok(()));
    assert_eq!(ledger.mark_terminal(id), Ok(()));
    assert_eq!(
        ledger.mark_terminal(id),
        Err(CompletionLedgerError::AlreadyCompleted)
    );
}
