//! Scenarios for lossless core-to-engine group-offset translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, GroupOffsetBrokerError as CoreBrokerError, GroupOffsetDescription as CoreDescription,
    GroupOffsetOutcome as CoreOutcome, ListConsumerGroupOffsetsBatch as CoreBatch,
    ListConsumerGroupOffsetsEffect as CoreEffect, ListConsumerGroupOffsetsInput as CoreInput,
    ListConsumerGroupOffsetsMachine as CoreMachine, ListConsumerGroupOffsetsPlan as CorePlan,
    ListConsumerGroupOffsetsTerminal as CoreTerminal, Moment, OperationId,
};

use super::{
    ListConsumerGroupOffsetsDeliveryStatus, ListConsumerGroupOffsetsFailureKind,
    ListConsumerGroupOffsetsOutcome, outcome::translate_terminal,
};

#[test]
fn throttle_order_nullable_values_and_partition_error_translate_exactly() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = CoreTerminal::Offsets(CoreBatch::new(
        73,
        vec![
            CoreOutcome::described(
                "audit".to_owned(),
                0,
                CoreDescription::new(None, None, None),
            ),
            CoreOutcome::described(
                "orders".to_owned(),
                1,
                CoreDescription::new(Some(42), Some(7), Some(String::new())),
            ),
            CoreOutcome::failed("orders".to_owned(), 2, CoreBrokerError::new(code)),
        ],
    ));
    let ListConsumerGroupOffsetsOutcome::Offsets(batch) = translate_terminal(terminal) else {
        panic!("offset batch expected");
    };
    let (throttle_time_ms, offsets) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    let (topic, partition, missing) = offsets[0].clone().into_parts();
    assert_eq!((topic.as_str(), partition), ("audit", 0));
    assert_eq!(
        missing
            .unwrap_or_else(|error| panic!("committed description expected: {error:?}"))
            .into_parts(),
        (None, None, None)
    );
    let (_topic, _partition, explicit) = offsets[1].clone().into_parts();
    assert_eq!(
        explicit
            .unwrap_or_else(|error| panic!("committed description expected: {error:?}"))
            .into_parts(),
        (Some(42), Some(7), Some(String::new()))
    );
    let (_topic, _partition, failed) = offsets[2].clone().into_parts();
    assert_eq!(
        failed
            .err()
            .unwrap_or_else(|| panic!("partition error expected"))
            .code(),
        -31_999
    );
}

#[test]
fn top_level_group_error_and_delivery_translate_without_reclassification() {
    let code = NonZeroI16::new(-31_777).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = terminal_from(CoreInput::BrokerRejected { code });
    let ListConsumerGroupOffsetsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("whole-operation failure expected");
    };
    assert_eq!(
        failure.kind(),
        ListConsumerGroupOffsetsFailureKind::Broker(-31_777)
    );
    assert_eq!(
        failure.delivery(),
        ListConsumerGroupOffsetsDeliveryStatus::PossiblySent
    );
}

fn terminal_from(input: CoreInput) -> CoreTerminal {
    let plan = CorePlan::new("payments".to_owned(), true)
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let mut machine = CoreMachine::new(OperationId::from_raw(29), Deadline::from_tick(20), plan);
    machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(CoreInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit core machine: {error}"));
    let transition = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("terminal core input: {error}"));
    let Some(CoreEffect::Complete { terminal, .. }) = transition.into_effect() else {
        panic!("terminal effect expected");
    };
    terminal
}
