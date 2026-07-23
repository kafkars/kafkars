//! Exact tail and generated-fact refusal scenarios.

use kafka_client_core::{
    DeliveryStatus, OperationId, ProducerCompletion, ProducerEffect, ProducerFailure, ProducerInput,
};

use super::{TailQuarantineError, TerminalQuarantine};

#[test]
fn oversized_tail_is_returned_exactly_without_mutating_the_bounded_owner() {
    let mut quarantine = TerminalQuarantine::new(1, 1);
    let oversized = vec![complete(3), complete(4)];

    let failure = quarantine
        .retain_committed_tail(oversized.clone())
        .err()
        .unwrap_or_else(|| panic!("oversized tail must be refused"));

    assert_eq!(failure.error(), TailQuarantineError::Capacity);
    assert_eq!(failure.into_tail(), oversized);
    assert_eq!(quarantine.committed_tail(), None);
    assert_eq!(quarantine.retained_tail_len(), 0);
}

#[test]
fn repeated_tail_refusal_never_overwrites_the_first_committed_tail() {
    let mut quarantine = TerminalQuarantine::new(1, 1);
    let first = vec![complete(1)];
    let second = vec![complete(2)];
    let third = vec![complete(3)];
    assert!(quarantine.retain_committed_tail(first.clone()).is_ok());

    let second_failure = quarantine
        .retain_committed_tail(second.clone())
        .err()
        .unwrap_or_else(|| panic!("second tail must remain caller-owned"));
    let third_failure = quarantine
        .retain_committed_tail(third.clone())
        .err()
        .unwrap_or_else(|| panic!("third tail must remain caller-owned"));

    assert_eq!(second_failure.error(), TailQuarantineError::DuplicateTail);
    assert_eq!(second_failure.into_tail(), second);
    assert_eq!(third_failure.error(), TailQuarantineError::DuplicateTail);
    assert_eq!(third_failure.into_tail(), third);
    assert_eq!(quarantine.committed_tail(), Some(&first[..]));
    assert_eq!(quarantine.retained_tail_len(), 1);
}

#[test]
fn generated_refusal_returns_exact_facts_without_overwriting_the_first_set() {
    let mut quarantine = TerminalQuarantine::new(1, 1);
    let first = vec![ProducerInput::ExecutionUnavailable];
    let repeated = vec![ProducerInput::FlushRequested];
    assert!(quarantine.retain_generated(first.clone()).is_ok());

    let failure = quarantine
        .retain_generated(repeated.clone())
        .err()
        .unwrap_or_else(|| panic!("repeated facts must remain caller-owned"));

    assert_eq!(failure.error(), TailQuarantineError::DuplicateTail);
    assert_eq!(failure.into_generated(), repeated);
    assert_eq!(quarantine.generated(), Some(&first[..]));
}

fn complete(operation: u64) -> ProducerEffect {
    ProducerEffect::Complete {
        operation_id: OperationId::from_raw(operation),
        completion: terminal(),
    }
}

fn terminal() -> ProducerCompletion {
    ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
        DeliveryStatus::NotSent,
    ))
}
