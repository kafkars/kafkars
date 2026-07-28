//! Transition scenarios for deterministic Admin `DeleteConsumerGroups` policy.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};
use core::num::NonZeroI16;

use super::{
    DELETE_CONSUMER_GROUPS_DIAGNOSTIC_BYTES, DeleteConsumerGroupsBrokerError,
    DeleteConsumerGroupsEffect, DeleteConsumerGroupsFailureKind, DeleteConsumerGroupsInput,
    DeleteConsumerGroupsMachine, DeleteConsumerGroupsOutcome, DeleteConsumerGroupsPlan,
    DeleteConsumerGroupsTarget, DeleteConsumerGroupsTerminal,
};

#[test]
fn groups_execute_sequentially_in_caller_order() {
    let mut machine = machine(["orders", "audit"]);
    let first = machine
        .apply(DeleteConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    assert!(matches!(
        first.into_effect(),
        Some(DeleteConsumerGroupsEffect::Submit { target, .. })
            if target.group_id() == "orders"
    ));
    machine
        .apply(DeleteConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let next = machine
        .apply(DeleteConsumerGroupsInput::BrokerResponded {
            throttle_time_ms: 4,
            outcome: DeleteConsumerGroupsOutcome::deleted("orders".to_owned()),
        })
        .unwrap_or_else(|error| panic!("response: {error}"));
    assert!(matches!(
        next.into_effect(),
        Some(DeleteConsumerGroupsEffect::Submit { target, .. })
            if target.group_id() == "audit"
    ));
}

#[test]
fn completed_transport_failure_and_unattempted_group_are_retained() {
    let mut machine = machine(["a", "b", "c"]);
    machine
        .apply(DeleteConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    machine
        .apply(DeleteConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    machine
        .apply(DeleteConsumerGroupsInput::BrokerResponded {
            throttle_time_ms: 11,
            outcome: DeleteConsumerGroupsOutcome::deleted("a".to_owned()),
        })
        .unwrap_or_else(|error| panic!("response: {error}"));
    machine
        .apply(DeleteConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second: {error}"));
    let terminal = machine
        .apply(DeleteConsumerGroupsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("transport: {error}"));
    let Some(DeleteConsumerGroupsEffect::Complete {
        terminal: DeleteConsumerGroupsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("expected failure terminal");
    };
    assert_eq!(failure.throttle_time_ms(), 11);
    assert_eq!(failure.completed().len(), 1);
    assert_eq!(failure.completed()[0].group_id(), "a");
    assert_eq!(failure.failed_target().group_id(), "b");
    assert_eq!(failure.unattempted().len(), 1);
    assert_eq!(failure.unattempted()[0].group_id(), "c");
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn completed_pre_driver_deadline_keeps_current_group_not_sent() {
    let mut machine = machine(["a", "b"]);
    machine
        .apply(DeleteConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DeleteConsumerGroupsInput::DriverAccepted))
        .and_then(|_| {
            machine.apply(DeleteConsumerGroupsInput::BrokerResponded {
                throttle_time_ms: 3,
                outcome: DeleteConsumerGroupsOutcome::deleted("a".to_owned()),
            })
        })
        .unwrap_or_else(|error| panic!("complete first group: {error}"));
    let terminal = machine
        .apply(DeleteConsumerGroupsInput::DeadlineElapsed)
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let Some(DeleteConsumerGroupsEffect::Complete {
        terminal: DeleteConsumerGroupsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("expected failure terminal");
    };
    assert_eq!(failure.completed()[0].group_id(), "a");
    assert_eq!(failure.failed_target().group_id(), "b");
    assert!(failure.unattempted().is_empty());
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn overlong_broker_diagnostic_becomes_possibly_sent_invalid_response() {
    let mut machine = machine(["orders"]);
    machine
        .apply(DeleteConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DeleteConsumerGroupsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit group deletion: {error}"));
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = machine
        .apply(DeleteConsumerGroupsInput::BrokerResponded {
            throttle_time_ms: 3,
            outcome: DeleteConsumerGroupsOutcome::failed(
                "orders".to_owned(),
                DeleteConsumerGroupsBrokerError::with_bounded_message(
                    code,
                    Some("x".repeat(DELETE_CONSUMER_GROUPS_DIAGNOSTIC_BYTES + 1)),
                    false,
                ),
            ),
        })
        .unwrap_or_else(|error| panic!("reject overlong diagnostic: {error}"));
    let Some(DeleteConsumerGroupsEffect::Complete {
        terminal: DeleteConsumerGroupsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("expected invalid-response terminal");
    };
    assert_eq!(
        failure.kind(),
        DeleteConsumerGroupsFailureKind::InvalidResponse
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

fn machine<const N: usize>(groups: [&str; N]) -> DeleteConsumerGroupsMachine {
    DeleteConsumerGroupsMachine::new(
        OperationId::from_raw(7),
        Deadline::from_tick(99),
        DeleteConsumerGroupsPlan::new(
            groups
                .into_iter()
                .map(|group| DeleteConsumerGroupsTarget::new(group.to_owned()))
                .collect(),
        )
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}
