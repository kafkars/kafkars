//! Lossless core-to-engine API-90 terminal translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DeliveryStatus, ListShareGroupOffsetDescription as CoreDescription,
    ListShareGroupOffsetOutcome as CorePartitionOutcome, ListShareGroupOffsetTarget as CoreTarget,
    ListShareGroupOffsetsBatch as CoreBatch, ListShareGroupOffsetsBatchOutcome as CoreBatchOutcome,
    ListShareGroupOffsetsBrokerError as CoreError, ListShareGroupOffsetsInput as CoreInput,
    ListShareGroupOffsetsMachine as CoreMachine,
    ListShareGroupOffsetsPartitionBrokerError as CorePartitionError,
    ListShareGroupOffsetsPlan as CorePlan, ListShareGroupOffsetsTerminal as CoreTerminal,
    ListShareGroupsOffsetsBatch as CoreGroupsBatch, Moment, OperationId,
};

use super::{
    ListShareGroupOffsetsBatchOutcome, ListShareGroupOffsetsDeliveryStatus,
    ListShareGroupOffsetsFailureKind, ListShareGroupOffsetsOutcome, outcome::translate_terminal,
};

#[test]
fn caller_order_offsets_topic_ids_and_exact_partition_error_translate_once() {
    let terminal = CoreTerminal::Offsets(CoreBatch::new(
        73,
        vec![
            CorePartitionOutcome::described(
                "orders".to_owned(),
                [7; 16],
                2,
                CoreDescription::new(Some(41), Some(9), Some(3)),
            ),
            CorePartitionOutcome::failed(
                "audit".to_owned(),
                [8; 16],
                1,
                CorePartitionError::new(nonzero(-31_999), Some("bounded prefix".to_owned()), true),
            ),
        ],
    ));
    let ListShareGroupOffsetsOutcome::Offsets(batch) = translate_terminal(terminal) else {
        panic!("offset batch expected");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.offsets()[0].topic(), "orders");
    assert_eq!(batch.offsets()[0].topic_id(), [7; 16]);
    assert_eq!(batch.offsets()[0].partition(), 2);
    let description = batch.offsets()[0]
        .result()
        .as_ref()
        .unwrap_or_else(|error| panic!("description expected: {error:?}"));
    assert_eq!(
        (
            description.start_offset(),
            description.leader_epoch(),
            description.lag(),
        ),
        (Some(41), Some(9), Some(3))
    );
    let error = batch.offsets()[1].result().as_ref().unwrap_err();
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
    let ListShareGroupOffsetsOutcome::BrokerRejected(error) = translate_terminal(terminal) else {
        panic!("broker rejection expected");
    };

    assert_eq!(
        error.into_parts(),
        (19, -32_000, Some("share group rejected".to_owned()), false,)
    );
}

#[test]
fn mechanism_failure_preserves_authoritative_delivery_certainty() {
    let plan = CorePlan::selected(
        "share".to_owned(),
        vec![CoreTarget::new("orders".to_owned(), 2)],
    )
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
    let Some(kafka_client_core::ListShareGroupOffsetsEffect::Complete { terminal, .. }) = effect
    else {
        panic!("terminal expected");
    };
    let ListShareGroupOffsetsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("failure expected");
    };

    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            ListShareGroupOffsetsFailureKind::Transport,
            ListShareGroupOffsetsDeliveryStatus::PossiblySent,
        )
    );
}

#[test]
fn multi_group_terminal_preserves_group_order_and_maximum_throttle() {
    let first_error = CoreError::new(47, nonzero(15), Some("not coordinator".to_owned()), false);
    let terminal = CoreTerminal::Batch(CoreGroupsBatch::new(
        47,
        vec![
            CoreBatchOutcome::broker_rejected("share-a".to_owned(), first_error),
            CoreBatchOutcome::offsets(
                "share-b".to_owned(),
                CoreBatch::new(
                    17,
                    vec![CorePartitionOutcome::described(
                        "audit".to_owned(),
                        [8; 16],
                        0,
                        CoreDescription::new(Some(9), None, Some(1)),
                    )],
                ),
            ),
        ],
    ));
    let ListShareGroupOffsetsOutcome::Batch(batch) = translate_terminal(terminal) else {
        panic!("group batch expected");
    };

    assert_eq!(batch.throttle_time_ms(), 47);
    assert_eq!(batch.outcomes()[0].group_id(), "share-a");
    assert_eq!(batch.outcomes()[1].group_id(), "share-b");
    assert!(matches!(
        &batch.outcomes()[0],
        ListShareGroupOffsetsBatchOutcome::BrokerRejected { error, .. }
            if error.code() == 15 && error.message() == Some("not coordinator")
    ));
    assert!(matches!(
        &batch.outcomes()[1],
        ListShareGroupOffsetsBatchOutcome::Offsets { offsets, .. }
            if offsets.offsets()[0].topic() == "audit"
    ));
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}
