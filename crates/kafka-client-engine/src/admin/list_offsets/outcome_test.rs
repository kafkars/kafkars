//! Scenarios for lossless core-to-engine Admin `ListOffsets` translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminListOffset as CoreValue, AdminListOffsetBrokerError as CoreBrokerError,
    AdminListOffsetOutcome as CoreOutcome, AdminListOffsetSpec as CoreSpec,
    AdminListOffsetTarget as CoreTarget, AdminListOffsetsBatch as CoreBatch,
    AdminListOffsetsEffect as CoreEffect, AdminListOffsetsInput as CoreInput,
    AdminListOffsetsMachine as CoreMachine, AdminListOffsetsPlan as CorePlan,
    AdminListOffsetsTerminal as CoreTerminal, Deadline, DeliveryStatus, Moment, OperationId,
};

use super::{
    AdminListOffsetsDeliveryStatus, AdminListOffsetsFailureKind, AdminListOffsetsOutcome,
    outcome::translate_terminal,
};

#[test]
fn throttle_order_optional_values_and_partition_error_translate_exactly() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = CoreTerminal::Listed(CoreBatch::new(
        73,
        vec![
            CoreOutcome::listed("audit".to_owned(), 0, CoreValue::new(None, None, None)),
            CoreOutcome::listed(
                "orders".to_owned(),
                1,
                CoreValue::new(Some(42), Some(1_234), Some(7)),
            ),
            CoreOutcome::failed("orders".to_owned(), 2, CoreBrokerError::new(code)),
        ],
    ));
    let AdminListOffsetsOutcome::Offsets(batch) = translate_terminal(terminal) else {
        panic!("offset batch expected");
    };
    let (throttle_time_ms, offsets) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    let (topic, partition, missing) = offsets[0].clone().into_parts();
    assert_eq!((topic.as_str(), partition), ("audit", 0));
    assert_eq!(
        missing
            .unwrap_or_else(|error| panic!("description expected: {error:?}"))
            .into_parts(),
        (None, None, None)
    );
    let (_topic, _partition, explicit) = offsets[1].clone().into_parts();
    assert_eq!(
        explicit
            .unwrap_or_else(|error| panic!("description expected: {error:?}"))
            .into_parts(),
        (Some(42), Some(1_234), Some(7))
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
fn whole_failure_and_delivery_translate_without_reclassification() {
    let terminal = failed_terminal(CoreInput::ProtocolIncompatible {
        delivery: DeliveryStatus::NotSent,
    });
    let AdminListOffsetsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("whole-operation failure expected");
    };
    assert_eq!(failure.kind(), AdminListOffsetsFailureKind::Compatibility);
    assert_eq!(failure.delivery(), AdminListOffsetsDeliveryStatus::NotSent);
}

fn failed_terminal(input: CoreInput) -> CoreTerminal {
    let plan = CorePlan::new(vec![CoreTarget::new(
        "orders".to_owned(),
        0,
        CoreSpec::Latest,
    )])
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
