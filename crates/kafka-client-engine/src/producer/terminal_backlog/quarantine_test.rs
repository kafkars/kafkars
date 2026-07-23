//! First-poison immutability and hard quarantine-bound scenarios.

use kafka_client_core::{
    DeliveryStatus, OperationId, ProducerCompletion, ProducerEffect, ProducerFailure,
};

use super::{RejectedTerminal, TerminalPoisonSlot, TerminalQuarantine};
use crate::producer::{
    ProducerHostInvariantError,
    binding::CompletionBindingError,
    host_limits_test::{start, valid_limits},
};

#[test]
fn quarantine_hard_bounds_preserve_refused_tokens_in_failures() {
    let mut poison = TerminalPoisonSlot::empty();
    assert!(poison.retain(rejected(1)).is_ok());
    let refused = poison
        .retain(rejected(2))
        .err()
        .unwrap_or_else(|| panic!("second poison must return exact evidence"));
    let mut quarantine = TerminalQuarantine::new(1, 1);
    assert!(quarantine.retain_terminal(refused).is_ok());
    assert_eq!(quarantine.terminal_len(), 1);
    assert_eq!(
        quarantine
            .first_terminal()
            .map(RejectedTerminal::operation_id),
        Some(OperationId::from_raw(2))
    );
    assert_eq!(
        quarantine
            .first_terminal()
            .map(RejectedTerminal::completion),
        Some(terminal())
    );

    let effect = ProducerEffect::Complete {
        operation_id: OperationId::from_raw(3),
        completion: terminal(),
    };
    assert!(quarantine.retain_committed_tail(vec![effect]).is_ok());
    assert_eq!(quarantine.retained_tail_len(), 1);
    assert_eq!(quarantine.retained_len(), 2);
}

#[test]
fn refused_tail_complete_remains_in_the_single_committed_tail_owner() {
    let mut host = start(valid_limits());
    let refused = ProducerEffect::Complete {
        operation_id: OperationId::from_raw(99),
        completion: terminal(),
    };

    assert_eq!(
        host.quarantine_committed_tail(vec![refused]),
        Err(ProducerHostInvariantError::Binding(
            CompletionBindingError::UnknownOperation
        ))
    );
    assert_eq!(host.stats().terminal_backlog, 0);
    assert_eq!(
        host.terminal_quarantine.committed_tail(),
        Some(&[refused][..])
    );
    assert!(host.verify_terminal_cleanup().is_err());
    host.clear_terminal_evidence();
    assert!(host.verify_terminal_cleanup().is_ok());
}

fn rejected(operation: u64) -> RejectedTerminal {
    RejectedTerminal::new(
        OperationId::from_raw(operation),
        None,
        terminal(),
        ProducerHostInvariantError::Binding(CompletionBindingError::UnknownOperation),
    )
}

fn terminal() -> ProducerCompletion {
    ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
        DeliveryStatus::NotSent,
    ))
}
