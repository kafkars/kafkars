//! Ownership scenarios for producer-local terminal values.

use kafka_client_core::{DeliveryStatus, ProducerCompletion, ProducerFailure};

use super::terminal::ProducerTerminal;

#[test]
fn producer_terminal_variants_preserve_their_operation_kind() {
    let completion = ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
        DeliveryStatus::PossiblySent,
    ));

    assert_eq!(
        ProducerTerminal::record(completion),
        ProducerTerminal::Record(completion)
    );
    assert_eq!(
        ProducerTerminal::flush_completed(),
        ProducerTerminal::FlushCompleted
    );
    assert_eq!(
        ProducerTerminal::execution_unavailable(),
        ProducerTerminal::ExecutionUnavailable
    );
}
