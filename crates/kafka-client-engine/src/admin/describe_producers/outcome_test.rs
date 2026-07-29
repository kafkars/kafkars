//! Scenarios for lossless core-to-engine Admin `DescribeProducers` translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminDescribeProducerBrokerError as CoreBrokerError,
    AdminDescribeProducerOutcome as CoreOutcome, AdminDescribeProducerTarget as CoreTarget,
    AdminDescribeProducersBatch as CoreBatch, AdminDescribeProducersEffect as CoreEffect,
    AdminDescribeProducersInput as CoreInput, AdminDescribeProducersMachine as CoreMachine,
    AdminDescribeProducersPlan as CorePlan, AdminDescribeProducersTerminal as CoreTerminal,
    AdminProducerState as CoreProducerState, Deadline, DeliveryStatus, Moment, OperationId,
};

use super::{
    AdminDescribeProducersDeliveryStatus, AdminDescribeProducersFailureKind,
    AdminDescribeProducersOutcome, outcome::translate_terminal,
};

#[test]
fn throttle_order_states_and_partition_error_translate_exactly() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = CoreTerminal::Described(CoreBatch::new(
        73,
        vec![
            CoreOutcome::described(
                "orders".to_owned(),
                2,
                vec![CoreProducerState::new(41, 3, -1, -1, 7, None)],
            ),
            CoreOutcome::broker_failed(
                "audit".to_owned(),
                0,
                CoreBrokerError::new(code, Some("denied".to_owned()), false),
            ),
        ],
    ));
    let AdminDescribeProducersOutcome::Described(batch) = translate_terminal(terminal) else {
        panic!("described batch expected");
    };
    let (throttle_time_ms, results) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    let (topic, partition, described) = results[0].clone().into_parts();
    assert_eq!((topic.as_str(), partition), ("orders", 2));
    let producers = described.unwrap_or_else(|error| panic!("states expected: {error:?}"));
    assert_eq!(producers[0].into_parts(), (41, 3, -1, -1, 7, None));
    let (topic, partition, failed) = results[1].clone().into_parts();
    assert_eq!((topic.as_str(), partition), ("audit", 0));
    let error = failed
        .err()
        .unwrap_or_else(|| panic!("broker error expected"));
    assert_eq!(
        error.into_parts(),
        (-31_999, Some("denied".to_owned()), false)
    );
}

#[test]
fn whole_failure_and_delivery_translate_without_reclassification() {
    let terminal = failed_terminal(CoreInput::ProtocolIncompatible {
        delivery: DeliveryStatus::NotSent,
    });
    let AdminDescribeProducersOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("whole-operation failure expected");
    };
    assert_eq!(
        failure.kind(),
        AdminDescribeProducersFailureKind::Compatibility
    );
    assert_eq!(
        failure.delivery(),
        AdminDescribeProducersDeliveryStatus::NotSent
    );
}

fn failed_terminal(input: CoreInput) -> CoreTerminal {
    let plan = CorePlan::new(vec![CoreTarget::new("orders".to_owned(), 0)], None)
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
