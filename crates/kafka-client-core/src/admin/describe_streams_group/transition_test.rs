//! Scenarios for API-89 lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeStreamsGroupBrokerError, DescribeStreamsGroupDescription, DescribeStreamsGroupEffect,
    DescribeStreamsGroupFailureKind, DescribeStreamsGroupInput, DescribeStreamsGroupMachine,
    DescribeStreamsGroupMachineError, DescribeStreamsGroupOutcome, DescribeStreamsGroupPlan,
    DescribeStreamsGroupResult, DescribeStreamsGroupState, DescribeStreamsGroupTerminal,
    DescribeStreamsGroupTransition,
};

#[test]
fn original_deadline_and_plan_cross_the_only_submit_effect() {
    let mut machine = machine(20);
    let transition = machine
        .apply(DescribeStreamsGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    let Some(DescribeStreamsGroupEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("submit expected");
    };

    assert_eq!(operation_id, OperationId::from_raw(89));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan.group_id(), "streams-workers");
    assert!(plan.include_authorized_operations());
    assert!(plan.include_topology_description());
}

#[test]
fn deadline_and_driver_failures_preserve_certainty_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(DescribeStreamsGroupInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed: {error}")),
        DescribeStreamsGroupFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );
    for (input, kind, delivery) in [
        (
            DescribeStreamsGroupInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DescribeStreamsGroupFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeStreamsGroupInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeStreamsGroupFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            DescribeStreamsGroupInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DescribeStreamsGroupFailureKind::Transport,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = submitted();
        assert_failure(
            machine
                .apply(input)
                .unwrap_or_else(|error| panic!("failure: {error}")),
            kind,
            delivery,
        );
    }
}

#[test]
fn terminal_is_assigned_once() {
    let mut machine = submitted();
    machine
        .apply(DescribeStreamsGroupInput::InvalidResponse)
        .unwrap_or_else(|error| panic!("terminal: {error}"));
    assert_eq!(machine.state(), DescribeStreamsGroupState::Completed);
    assert_eq!(
        machine.apply(DescribeStreamsGroupInput::InvalidResponse),
        Err(DescribeStreamsGroupMachineError::AlreadyCompleted)
    );
}

#[test]
fn two_group_batch_continues_after_rejection_and_preserves_order_and_max_throttle() {
    let mut machine = batch_machine(40);
    let first = machine
        .apply(DescribeStreamsGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start first: {error}"));
    assert_submit(first, "orders", 40);
    machine
        .apply(DescribeStreamsGroupInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first: {error}"));

    let second = machine
        .apply(DescribeStreamsGroupInput::BrokerRejected {
            error: broker_error(29, 15),
        })
        .unwrap_or_else(|error| panic!("reject first: {error}"));
    assert_submit(second, "audit", 40);
    machine
        .apply(DescribeStreamsGroupInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second: {error}"));

    let terminal = machine
        .apply(DescribeStreamsGroupInput::BrokerResponded {
            result: result("audit", 7),
        })
        .unwrap_or_else(|error| panic!("describe second: {error}"));
    let Some(DescribeStreamsGroupEffect::Complete {
        terminal: DescribeStreamsGroupTerminal::Batch(batch),
        ..
    }) = terminal.into_effect()
    else {
        panic!("batch terminal expected");
    };

    assert_eq!(batch.throttle_time_ms(), 29);
    assert_eq!(batch.outcomes().len(), 2);
    assert_eq!(batch.outcomes()[0].group_id(), "orders");
    assert_eq!(batch.outcomes()[1].group_id(), "audit");
    let DescribeStreamsGroupOutcome::BrokerRejected { error, .. } = &batch.outcomes()[0] else {
        panic!("first group rejection expected");
    };
    assert_eq!(error.code(), 15);
    assert!(matches!(
        batch.outcomes().get(1),
        Some(DescribeStreamsGroupOutcome::Described(_))
    ));
}

#[test]
fn original_deadline_applies_to_later_groups_with_aggregate_delivery() {
    let mut machine = batch_machine(20);
    machine
        .apply(DescribeStreamsGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DescribeStreamsGroupInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit first: {error}"));
    let next = machine
        .apply(DescribeStreamsGroupInput::BrokerResponded {
            result: result("orders", 3),
        })
        .unwrap_or_else(|error| panic!("describe first: {error}"));
    assert_submit(next, "audit", 20);

    assert_failure(
        machine
            .apply(DescribeStreamsGroupInput::DeadlineElapsed)
            .unwrap_or_else(|error| panic!("expire second: {error}")),
        DescribeStreamsGroupFailureKind::DeadlineElapsed,
        DeliveryStatus::PossiblySent,
    );
}

fn machine(deadline: u64) -> DescribeStreamsGroupMachine {
    DescribeStreamsGroupMachine::new(
        OperationId::from_raw(89),
        Deadline::from_tick(deadline),
        DescribeStreamsGroupPlan::new("streams-workers".to_owned(), true, true)
            .unwrap_or_else(|error| panic!("plan: {error}")),
    )
}

fn batch_machine(deadline: u64) -> DescribeStreamsGroupMachine {
    DescribeStreamsGroupMachine::new(
        OperationId::from_raw(89),
        Deadline::from_tick(deadline),
        DescribeStreamsGroupPlan::new_batch(
            vec!["orders".to_owned(), "audit".to_owned()],
            false,
            false,
        )
        .unwrap_or_else(|error| panic!("batch plan: {error}")),
    )
}

fn submitted() -> DescribeStreamsGroupMachine {
    let mut machine = machine(20);
    machine
        .apply(DescribeStreamsGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DescribeStreamsGroupInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit: {error}"));
    machine
}

fn assert_submit(transition: DescribeStreamsGroupTransition, group_id: &str, deadline: u64) {
    let Some(DescribeStreamsGroupEffect::Submit {
        operation_id,
        deadline: actual_deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("submit expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(89));
    assert_eq!(actual_deadline, Deadline::from_tick(deadline));
    assert_eq!(plan.group_ids(), [group_id]);
}

fn result(group_id: &str, throttle_time_ms: u32) -> DescribeStreamsGroupResult {
    DescribeStreamsGroupResult::new(
        throttle_time_ms,
        DescribeStreamsGroupDescription::new(
            group_id.to_owned(),
            "Stable".to_owned(),
            1,
            2,
            None,
            Vec::new(),
            None,
            None,
            None,
        ),
    )
}

fn broker_error(throttle_time_ms: u32, code: i16) -> DescribeStreamsGroupBrokerError {
    DescribeStreamsGroupBrokerError::new(
        throttle_time_ms,
        NonZeroI16::new(code).unwrap_or_else(|| panic!("broker code must be nonzero")),
        Some("group rejected".to_owned()),
        false,
    )
}

fn assert_failure(
    transition: DescribeStreamsGroupTransition,
    kind: DescribeStreamsGroupFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(DescribeStreamsGroupEffect::Complete {
        terminal: DescribeStreamsGroupTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
