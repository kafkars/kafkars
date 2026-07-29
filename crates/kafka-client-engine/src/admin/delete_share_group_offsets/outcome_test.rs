//! Lossless core-to-engine API-92 terminal translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DeleteShareGroupOffsetsBatch as CoreBatch,
    DeleteShareGroupOffsetsBrokerError as CoreError, DeleteShareGroupOffsetsInput as CoreInput,
    DeleteShareGroupOffsetsMachine as CoreMachine, DeleteShareGroupOffsetsPlan as CorePlan,
    DeleteShareGroupOffsetsTerminal as CoreTerminal,
    DeleteShareGroupOffsetsTopicBrokerError as CoreTopicError,
    DeleteShareGroupOffsetsTopicOutcome as CoreTopicOutcome, DeliveryStatus, Moment, OperationId,
};

use super::{
    DeleteShareGroupOffsetsDeliveryStatus, DeleteShareGroupOffsetsFailureKind,
    DeleteShareGroupOffsetsOutcome, outcome::translate_terminal,
};

#[test]
fn caller_order_topic_ids_and_exact_topic_error_translate_once() {
    let terminal = CoreTerminal::Deleted(CoreBatch::new(
        73,
        vec![
            CoreTopicOutcome::deleted("orders".to_owned(), [7; 16]),
            CoreTopicOutcome::failed(
                "audit".to_owned(),
                CoreTopicError::new(nonzero(-31_999), Some("bounded prefix".to_owned()), true),
            ),
        ],
    ));
    let DeleteShareGroupOffsetsOutcome::Deleted(batch) = translate_terminal(terminal) else {
        panic!("deleted batch expected");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.topics()[0].topic(), "orders");
    assert_eq!(batch.topics()[0].result(), &Ok([7; 16]));
    let error = batch.topics()[1]
        .result()
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("topic error expected"));
    assert_eq!(error.code(), -31_999);
    assert_eq!(error.message(), Some("bounded prefix"));
    assert!(error.message_truncated());
}

#[test]
fn top_level_rejection_preserves_throttle_code_and_diagnostic() {
    let terminal = CoreTerminal::BrokerRejected(CoreError::new(
        19,
        nonzero(-32_000),
        Some("share group rejected".to_owned()),
        false,
    ));
    let DeleteShareGroupOffsetsOutcome::BrokerRejected(error) = translate_terminal(terminal) else {
        panic!("broker rejection expected");
    };

    assert_eq!(
        error.into_parts(),
        (19, -32_000, Some("share group rejected".to_owned()), false,)
    );
}

#[test]
fn mechanism_failure_preserves_authoritative_delivery_certainty() {
    let plan = CorePlan::new("share".to_owned(), vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let mut machine = CoreMachine::new(OperationId::from_raw(92), Deadline::from_tick(20), plan);
    machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(CoreInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit: {error}"));
    let effect = machine
        .apply(CoreInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("settle: {error}"))
        .into_effect();
    let Some(kafka_client_core::DeleteShareGroupOffsetsEffect::Complete { terminal, .. }) = effect
    else {
        panic!("terminal expected");
    };
    let DeleteShareGroupOffsetsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("failure expected");
    };

    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteShareGroupOffsetsFailureKind::Transport,
            DeleteShareGroupOffsetsDeliveryStatus::PossiblySent,
        )
    );
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}
