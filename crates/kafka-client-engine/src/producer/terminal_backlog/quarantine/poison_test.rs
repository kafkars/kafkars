//! Immutable poison-slot ownership scenarios.

use kafka_client_core::{DeliveryStatus, OperationId, ProducerCompletion, ProducerFailure};

use super::{RejectedTerminal, TerminalPoisonSlot};
use crate::producer::{ProducerHostInvariantError, binding::OperationBindingError};

#[test]
fn poison_refuses_to_overwrite_and_returns_the_second_exact_terminal() {
    let mut poison = TerminalPoisonSlot::empty();

    assert!(poison.retain(rejected(1)).is_ok());
    let failure = poison
        .retain(rejected(2))
        .err()
        .unwrap_or_else(|| panic!("second poison must retain ownership in its failure"));

    assert_eq!(
        poison.evidence().map(RejectedTerminal::operation_id),
        Some(OperationId::from_raw(1))
    );
    assert_eq!(
        failure.into_evidence().operation_id(),
        OperationId::from_raw(2)
    );
}

fn rejected(operation: u64) -> RejectedTerminal {
    RejectedTerminal::new(
        OperationId::from_raw(operation),
        None,
        terminal(),
        ProducerHostInvariantError::Binding(OperationBindingError::UnknownOperation),
    )
}

fn terminal() -> ProducerCompletion {
    ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
        DeliveryStatus::NotSent,
    ))
}
