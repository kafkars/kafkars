//! Scenarios for API-77 lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeShareGroupBrokerError, DescribeShareGroupDescription, DescribeShareGroupEffect,
    DescribeShareGroupFailureKind, DescribeShareGroupInput, DescribeShareGroupMachine,
    DescribeShareGroupMachineError, DescribeShareGroupOutcome, DescribeShareGroupPlan,
    DescribeShareGroupResult, DescribeShareGroupState, DescribeShareGroupTerminal,
    DescribeShareGroupTransition,
};

#[test]
fn original_deadline_and_plan_cross_the_only_submit_effect() {
    let mut machine = machine(20);
    let transition = machine
        .apply(DescribeShareGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    let Some(DescribeShareGroupEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("submit expected");
    };

    assert_eq!(operation_id, OperationId::from_raw(77));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan.group_id(), "share-workers");
    assert!(plan.include_authorized_operations());
}

#[test]
fn deadline_and_driver_failures_preserve_certainty_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(DescribeShareGroupInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed: {error}")),
        DescribeShareGroupFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );
    for (input, kind, delivery) in [
        (
            DescribeShareGroupInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DescribeShareGroupFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeShareGroupInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeShareGroupFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            DescribeShareGroupInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DescribeShareGroupFailureKind::Transport,
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
        .apply(DescribeShareGroupInput::InvalidResponse)
        .unwrap_or_else(|error| panic!("terminal: {error}"));
    assert_eq!(machine.state(), DescribeShareGroupState::Completed);
    assert_eq!(
        machine.apply(DescribeShareGroupInput::InvalidResponse),
        Err(DescribeShareGroupMachineError::AlreadyCompleted)
    );
}

#[test]
fn two_groups_submit_sequentially_and_complete_in_caller_order_with_maximum_throttle() {
    let mut machine = batch_machine(["orders-share", "audit-share"], 50);
    let first = machine
        .apply(DescribeShareGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    assert_submit(first, "orders-share", 50);

    machine
        .apply(DescribeShareGroupInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first: {error}"));
    let second = machine
        .apply(DescribeShareGroupInput::BrokerResponded {
            result: described("orders-share", 7),
        })
        .unwrap_or_else(|error| panic!("first response: {error}"));
    assert_submit(second, "audit-share", 50);

    machine
        .apply(DescribeShareGroupInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second: {error}"));
    let complete = machine
        .apply(DescribeShareGroupInput::BrokerResponded {
            result: described("audit-share", 19),
        })
        .unwrap_or_else(|error| panic!("second response: {error}"));
    let Some(DescribeShareGroupEffect::Complete {
        terminal: DescribeShareGroupTerminal::Batch(batch),
        ..
    }) = complete.into_effect()
    else {
        panic!("batch terminal expected");
    };
    assert_eq!(batch.throttle_time_ms(), 19);
    assert_eq!(batch.outcomes().len(), 2);
    assert_eq!(batch.outcomes()[0].group_id(), "orders-share");
    assert_eq!(batch.outcomes()[1].group_id(), "audit-share");
    assert!(matches!(
        batch.outcomes(),
        [
            DescribeShareGroupOutcome::Described(_),
            DescribeShareGroupOutcome::Described(_)
        ]
    ));
    assert_eq!(machine.state(), DescribeShareGroupState::Completed);
}

#[test]
fn per_group_broker_error_does_not_abort_remaining_groups() {
    let mut machine = batch_machine(["missing-share", "orders-share"], 50);
    machine
        .apply(DescribeShareGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DescribeShareGroupInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("first submission: {error}"));
    let second = machine
        .apply(DescribeShareGroupInput::BrokerRejected {
            error: DescribeShareGroupBrokerError::new(
                23,
                NonZeroI16::new(69).unwrap_or_else(|| panic!("nonzero")),
                Some("group missing".to_owned()),
                false,
            ),
        })
        .unwrap_or_else(|error| panic!("broker rejection: {error}"));
    assert_submit(second, "orders-share", 50);

    machine
        .apply(DescribeShareGroupInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second: {error}"));
    let complete = machine
        .apply(DescribeShareGroupInput::BrokerResponded {
            result: described("orders-share", 3),
        })
        .unwrap_or_else(|error| panic!("second response: {error}"));
    let Some(DescribeShareGroupEffect::Complete {
        terminal: DescribeShareGroupTerminal::Batch(batch),
        ..
    }) = complete.into_effect()
    else {
        panic!("batch terminal expected");
    };
    assert_eq!(batch.throttle_time_ms(), 23);
    assert!(matches!(
        &batch.outcomes()[0],
        DescribeShareGroupOutcome::BrokerRejected { error, .. } if error.code() == 69
    ));
    assert!(matches!(
        &batch.outcomes()[1],
        DescribeShareGroupOutcome::Described(_)
    ));
}

#[test]
fn later_deadline_failure_aggregates_prior_delivery_without_new_deadline() {
    let mut machine = batch_machine(["orders-share", "audit-share"], 50);
    machine
        .apply(DescribeShareGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DescribeShareGroupInput::DriverAccepted))
        .and_then(|_| {
            machine.apply(DescribeShareGroupInput::BrokerResponded {
                result: described("orders-share", 11),
            })
        })
        .unwrap_or_else(|error| panic!("first group: {error}"));

    assert_failure(
        machine
            .apply(DescribeShareGroupInput::DeadlineElapsed)
            .unwrap_or_else(|error| panic!("second deadline: {error}")),
        DescribeShareGroupFailureKind::DeadlineElapsed,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn singular_plan_preserves_legacy_terminal_shape() {
    let mut machine = submitted();
    let transition = machine
        .apply(DescribeShareGroupInput::BrokerResponded {
            result: described("share-workers", 17),
        })
        .unwrap_or_else(|error| panic!("response: {error}"));

    assert!(matches!(
        transition.into_effect(),
        Some(DescribeShareGroupEffect::Complete {
            terminal: DescribeShareGroupTerminal::Described(_),
            ..
        })
    ));
}

#[test]
fn one_element_batch_uses_the_batch_terminal_shape() {
    let mut machine = batch_machine(["share-workers"], 50);
    let transition = machine
        .apply(DescribeShareGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DescribeShareGroupInput::DriverAccepted))
        .and_then(|_| {
            machine.apply(DescribeShareGroupInput::BrokerResponded {
                result: described("share-workers", 5),
            })
        })
        .unwrap_or_else(|error| panic!("one-element batch: {error}"));

    assert!(matches!(
        transition.into_effect(),
        Some(DescribeShareGroupEffect::Complete {
            terminal: DescribeShareGroupTerminal::Batch(_),
            ..
        })
    ));
}

fn machine(deadline: u64) -> DescribeShareGroupMachine {
    DescribeShareGroupMachine::new(
        OperationId::from_raw(77),
        Deadline::from_tick(deadline),
        DescribeShareGroupPlan::new("share-workers".to_owned(), true)
            .unwrap_or_else(|error| panic!("plan: {error}")),
    )
}

fn submitted() -> DescribeShareGroupMachine {
    let mut machine = machine(20);
    machine
        .apply(DescribeShareGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DescribeShareGroupInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit: {error}"));
    machine
}

fn batch_machine<const N: usize>(group_ids: [&str; N], deadline: u64) -> DescribeShareGroupMachine {
    DescribeShareGroupMachine::new(
        OperationId::from_raw(77),
        Deadline::from_tick(deadline),
        DescribeShareGroupPlan::new_batch(
            group_ids.into_iter().map(str::to_owned).collect(),
            false,
        )
        .unwrap_or_else(|error| panic!("batch plan: {error}")),
    )
}

fn described(group_id: &str, throttle_time_ms: u32) -> DescribeShareGroupResult {
    DescribeShareGroupResult::new(
        throttle_time_ms,
        DescribeShareGroupDescription::new(
            group_id.to_owned(),
            "Stable".to_owned(),
            1,
            2,
            "uniform".to_owned(),
            Vec::new(),
            None,
        ),
    )
}

fn assert_submit(transition: DescribeShareGroupTransition, group_id: &str, deadline: u64) {
    let Some(DescribeShareGroupEffect::Submit {
        deadline: actual_deadline,
        plan,
        ..
    }) = transition.into_effect()
    else {
        panic!("submit expected");
    };
    assert_eq!(actual_deadline, Deadline::from_tick(deadline));
    assert_eq!(plan.group_ids(), [group_id]);
}

fn assert_failure(
    transition: DescribeShareGroupTransition,
    kind: DescribeShareGroupFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(DescribeShareGroupEffect::Complete {
        terminal: DescribeShareGroupTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
