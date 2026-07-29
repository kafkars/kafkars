//! Scenarios for lossless core-to-engine Admin `FenceProducers` translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminFenceProducerBrokerError as CoreBrokerError, AdminFenceProducerOutcome as CoreOutcome,
    AdminFenceProducersBatch as CoreBatch, AdminFenceProducersEffect as CoreEffect,
    AdminFenceProducersInput as CoreInput, AdminFenceProducersMachine as CoreMachine,
    AdminFenceProducersPlan as CorePlan, AdminFenceProducersTerminal as CoreTerminal,
    AdminFencedProducerIdentity as CoreIdentity, Deadline, DeliveryStatus, Moment, OperationId,
};

use super::{
    AdminFenceProducersDeliveryStatus, AdminFenceProducersFailureKind, AdminFenceProducersOutcome,
    outcome::translate_terminal,
};

#[test]
fn throttle_order_identity_and_exact_broker_error_translate() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let identity =
        CoreIdentity::try_new(41, 3).unwrap_or_else(|| panic!("valid producer identity"));
    let terminal = CoreTerminal::Fenced(CoreBatch::new(
        73,
        vec![
            CoreOutcome::fenced("orders-writer".to_owned(), identity),
            CoreOutcome::broker_failed("audit-writer".to_owned(), CoreBrokerError::new(code)),
        ],
    ));
    let AdminFenceProducersOutcome::Fenced(batch) = translate_terminal(terminal) else {
        panic!("fenced batch expected");
    };
    let (throttle_time_ms, results) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    let (transactional_id, fenced) = results[0].clone().into_parts();
    assert_eq!(transactional_id, "orders-writer");
    assert_eq!(
        fenced
            .unwrap_or_else(|error| panic!("identity expected: {error:?}"))
            .into_parts(),
        (41, 3)
    );
    let (transactional_id, failed) = results[1].clone().into_parts();
    assert_eq!(transactional_id, "audit-writer");
    assert_eq!(
        failed
            .err()
            .unwrap_or_else(|| panic!("broker error expected"))
            .code(),
        -31_999
    );
}

#[test]
fn whole_failure_and_delivery_translate_without_reclassification() {
    let terminal = failed_terminal(CoreInput::ProtocolIncompatible {
        delivery: DeliveryStatus::NotSent,
    });
    let AdminFenceProducersOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("whole-operation failure expected");
    };
    assert_eq!(
        failure.kind(),
        AdminFenceProducersFailureKind::Compatibility
    );
    assert_eq!(
        failure.delivery(),
        AdminFenceProducersDeliveryStatus::NotSent
    );
}

fn failed_terminal(input: CoreInput) -> CoreTerminal {
    let plan = CorePlan::new(vec!["orders-writer".to_owned()])
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
