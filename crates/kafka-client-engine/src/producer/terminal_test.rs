//! Ownership scenarios for producer-local terminal values.

use kafka_client_core::{DeliveryStatus, ProducerCompletion, ProducerFailure};

use super::terminal::ProducerTerminal;

#[test]
fn record_terminal_moves_through_the_envelope_without_semantic_change() {
    let completion = ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
        DeliveryStatus::PossiblySent,
    ));

    assert_eq!(
        ProducerTerminal::record(completion).into_record(),
        completion
    );
}
