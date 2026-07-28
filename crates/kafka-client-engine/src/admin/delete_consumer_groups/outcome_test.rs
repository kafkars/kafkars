//! Scenarios for lossless core-to-engine Admin `DeleteConsumerGroups` translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DeleteConsumerGroupsBatch as CoreBatch,
    DeleteConsumerGroupsBrokerError as CoreBrokerError, DeleteConsumerGroupsEffect as CoreEffect,
    DeleteConsumerGroupsInput as CoreInput, DeleteConsumerGroupsMachine as CoreMachine,
    DeleteConsumerGroupsOutcome as CoreOutcome, DeleteConsumerGroupsPlan as CorePlan,
    DeleteConsumerGroupsTarget as CoreTarget, DeleteConsumerGroupsTerminal as CoreTerminal,
    DeliveryStatus, Moment, OperationId,
};

use super::{
    DeleteConsumerGroupsDeliveryStatus, DeleteConsumerGroupsFailureKind,
    DeleteConsumerGroupsOutcome, outcome::translate_terminal,
};

#[test]
fn throttle_order_and_group_error_translate_exactly() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = CoreTerminal::Deleted(CoreBatch::new(
        73,
        vec![
            CoreOutcome::deleted("audit".to_owned()),
            CoreOutcome::failed(
                "orders".to_owned(),
                CoreBrokerError::with_bounded_message(code, Some("group busy".to_owned()), true),
            ),
        ],
    ));
    let DeleteConsumerGroupsOutcome::Deleted(batch) = translate_terminal(terminal) else {
        panic!("deletion batch expected");
    };
    let (throttle_time_ms, groups) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    let (group_id, first) = groups[0].clone().into_parts();
    assert_eq!(group_id, "audit");
    assert!(first.is_ok());
    let (_group_id, failed) = groups[1].clone().into_parts();
    let failed = failed
        .err()
        .unwrap_or_else(|| panic!("group error expected"));
    assert_eq!(failed.code(), -31_999);
    assert_eq!(failed.message(), Some("group busy"));
    assert!(failed.message_truncated());
}

#[test]
fn whole_failure_and_delivery_translate_without_reclassification() {
    let terminal = failed_terminal(CoreInput::ProtocolIncompatible {
        delivery: DeliveryStatus::NotSent,
    });
    let DeleteConsumerGroupsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("whole-operation failure expected");
    };
    assert_eq!(
        failure.kind(),
        DeleteConsumerGroupsFailureKind::Compatibility
    );
    assert_eq!(
        failure.delivery(),
        DeleteConsumerGroupsDeliveryStatus::NotSent
    );
}

#[test]
fn partial_failure_preserves_completed_failed_and_unattempted_groups() {
    let plan = CorePlan::new(vec![
        CoreTarget::new("a".to_owned()),
        CoreTarget::new("b".to_owned()),
        CoreTarget::new("c".to_owned()),
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
                outcome: CoreOutcome::deleted("a".to_owned()),
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
    let DeleteConsumerGroupsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("partial failure expected");
    };
    let (kind, delivery, throttle, completed, failed, unattempted) = failure.into_parts();
    assert_eq!(kind, DeleteConsumerGroupsFailureKind::Transport);
    assert_eq!(delivery, DeleteConsumerGroupsDeliveryStatus::PossiblySent);
    assert_eq!(throttle, 5);
    assert_eq!(completed[0].clone().into_parts().0, "a");
    assert_eq!(failed, "b");
    assert_eq!(unattempted, vec!["c".to_owned()]);
}

fn failed_terminal(input: CoreInput) -> CoreTerminal {
    let plan = CorePlan::new(vec![CoreTarget::new("orders".to_owned())])
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
