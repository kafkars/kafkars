//! Scenarios for lossless core-to-engine Admin `DeleteRecords` translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DeleteRecordsBatch as CoreBatch, DeleteRecordsBrokerError as CoreBrokerError,
    DeleteRecordsEffect as CoreEffect, DeleteRecordsInput as CoreInput,
    DeleteRecordsMachine as CoreMachine, DeleteRecordsOutcome as CoreOutcome,
    DeleteRecordsPlan as CorePlan, DeleteRecordsTarget as CoreTarget,
    DeleteRecordsTerminal as CoreTerminal, DeletedRecords as CoreValue, DeliveryStatus, Moment,
    OperationId,
};

use super::{
    DeleteRecordsDeliveryStatus, DeleteRecordsFailureKind, DeleteRecordsOutcome,
    outcome::translate_terminal,
};

#[test]
fn throttle_order_optional_values_and_partition_error_translate_exactly() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = CoreTerminal::Deleted(CoreBatch::new(
        73,
        vec![
            CoreOutcome::deleted("audit".to_owned(), 0, CoreValue::new(0)),
            CoreOutcome::deleted("orders".to_owned(), 1, CoreValue::new(42)),
            CoreOutcome::failed("orders".to_owned(), 2, CoreBrokerError::new(code)),
        ],
    ));
    let DeleteRecordsOutcome::Deleted(batch) = translate_terminal(terminal) else {
        panic!("deletion batch expected");
    };
    let (throttle_time_ms, records) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    let (topic, partition, first) = records[0].clone().into_parts();
    assert_eq!((topic.as_str(), partition), ("audit", 0));
    assert_eq!(
        first
            .unwrap_or_else(|error| panic!("description expected: {error:?}"))
            .low_watermark(),
        0
    );
    let (_topic, _partition, explicit) = records[1].clone().into_parts();
    assert_eq!(
        explicit
            .unwrap_or_else(|error| panic!("description expected: {error:?}"))
            .low_watermark(),
        42
    );
    let (_topic, _partition, failed) = records[2].clone().into_parts();
    assert_eq!(
        failed
            .err()
            .unwrap_or_else(|| panic!("partition error expected"))
            .code(),
        -31_999
    );
}

#[test]
fn whole_failure_and_delivery_translate_without_reclassification() {
    let terminal = failed_terminal(CoreInput::ProtocolIncompatible {
        delivery: DeliveryStatus::NotSent,
    });
    let DeleteRecordsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("whole-operation failure expected");
    };
    assert_eq!(failure.kind(), DeleteRecordsFailureKind::Compatibility);
    assert_eq!(failure.delivery(), DeleteRecordsDeliveryStatus::NotSent);
}

#[test]
fn partial_failure_preserves_completed_failed_and_unattempted_targets() {
    let plan = CorePlan::new(vec![
        CoreTarget::new("a".to_owned(), 0, 91),
        CoreTarget::new("b".to_owned(), 1, 42),
        CoreTarget::new("c".to_owned(), 2, 17),
    ])
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let mut machine = CoreMachine::new(OperationId::from_raw(31), Deadline::from_tick(20), plan);
    machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(CoreInput::DriverAccepted))
        .and_then(|_| {
            machine.apply(CoreInput::BrokerResponded {
                throttle_time_ms: 5,
                outcome: CoreOutcome::deleted("a".to_owned(), 0, CoreValue::new(91)),
            })
        })
        .and_then(|_| machine.apply(CoreInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("complete first and submit second: {error}"));
    let transition = machine
        .apply(CoreInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("transport failure: {error}"));
    let Some(CoreEffect::Complete { terminal, .. }) = transition.into_effect() else {
        panic!("terminal expected");
    };
    let DeleteRecordsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("partial failure expected");
    };
    let (kind, delivery, throttle, completed, failed, unattempted) = failure.into_parts();
    assert_eq!(kind, DeleteRecordsFailureKind::Transport);
    assert_eq!(delivery, DeleteRecordsDeliveryStatus::PossiblySent);
    assert_eq!(throttle, 5);
    assert_eq!(completed[0].clone().into_parts().0, "a");
    assert_eq!(failed.into_parts(), ("b".to_owned(), 1, 42));
    assert_eq!(unattempted[0].clone().into_parts(), ("c".to_owned(), 2, 17));
}

fn failed_terminal(input: CoreInput) -> CoreTerminal {
    let plan = CorePlan::new(vec![CoreTarget::new("orders".to_owned(), 0, 91)])
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
        .unwrap_or_else(|error| panic!("terminal input: {error}"));
    let Some(CoreEffect::Complete { terminal, .. }) = transition.into_effect() else {
        panic!("terminal effect expected");
    };
    terminal
}
