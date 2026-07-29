//! Failure and aggregate-delivery scenarios for serial `DescribeConfigs` routes.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeConfigOutcome, DescribeConfigsBatch, DescribeConfigsEffect, DescribeConfigsFailureKind,
    DescribeConfigsInput, DescribeConfigsMachine, DescribeConfigsPlan,
    DescribeConfigsResourceQuery, DescribeConfigsTerminal,
};

#[test]
fn first_route_pre_driver_failures_are_definitely_not_sent() {
    let mut expired = machine(4);
    assert_failure(
        apply(
            &mut expired,
            DescribeConfigsInput::Start {
                now: Moment::from_tick(4),
            },
        ),
        DescribeConfigsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    apply(
        &mut rejected,
        DescribeConfigsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    assert_failure(
        apply(&mut rejected, DescribeConfigsInput::DriverRejected),
        DescribeConfigsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn first_route_driver_failures_preserve_authoritative_certainty() {
    let mut deadline = first_route_submitted();
    assert_failure(
        apply(
            &mut deadline,
            DescribeConfigsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
        ),
        DescribeConfigsFailureKind::DeadlineElapsed,
        DeliveryStatus::PossiblySent,
    );

    let mut transport = first_route_submitted();
    assert_failure(
        apply(
            &mut transport,
            DescribeConfigsInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
        ),
        DescribeConfigsFailureKind::Transport,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn later_route_not_sent_failures_preserve_aggregate_possibly_sent_truth() {
    for (input, kind) in [
        (
            DescribeConfigsInput::DriverRejected,
            DescribeConfigsFailureKind::DriverRejected,
        ),
        (
            DescribeConfigsInput::DeadlineElapsed,
            DescribeConfigsFailureKind::DeadlineElapsed,
        ),
    ] {
        let mut machine = second_route_awaiting();
        assert_failure(
            apply(&mut machine, input),
            kind,
            DeliveryStatus::PossiblySent,
        );
    }

    for (input, kind) in [
        (
            DescribeConfigsInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeConfigsFailureKind::Transport,
        ),
        (
            DescribeConfigsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeConfigsFailureKind::DeadlineElapsed,
        ),
        (
            DescribeConfigsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeConfigsFailureKind::Compatibility,
        ),
    ] {
        let mut machine = second_route_submitted();
        assert_failure(
            apply(&mut machine, input),
            kind,
            DeliveryStatus::PossiblySent,
        );
    }
}

#[test]
fn submitted_failure_categories_remain_distinct() {
    for (input, kind, delivery) in [
        (
            DescribeConfigsInput::InvalidResponse,
            DescribeConfigsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeConfigsInput::ResponseTooLarge,
            DescribeConfigsFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeConfigsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeConfigsFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
    ] {
        let mut machine = first_route_submitted();
        assert_failure(apply(&mut machine, input), kind, delivery);
    }
}

fn first_route_submitted() -> DescribeConfigsMachine {
    let mut machine = machine(20);
    apply(
        &mut machine,
        DescribeConfigsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    apply(&mut machine, DescribeConfigsInput::DriverAccepted);
    machine
}

fn second_route_awaiting() -> DescribeConfigsMachine {
    let mut machine = first_route_submitted();
    let transition = apply(
        &mut machine,
        DescribeConfigsInput::BrokerResponded {
            batch: DescribeConfigsBatch::new(
                3,
                vec![DescribeConfigOutcome::described(2, "orders", Vec::new())],
            ),
        },
    );
    assert!(matches!(
        transition.into_effect(),
        Some(DescribeConfigsEffect::Submit { .. })
    ));
    machine
}

fn second_route_submitted() -> DescribeConfigsMachine {
    let mut machine = second_route_awaiting();
    apply(&mut machine, DescribeConfigsInput::DriverAccepted);
    machine
}

fn machine(deadline: u64) -> DescribeConfigsMachine {
    let plan = DescribeConfigsPlan::new(
        vec![
            DescribeConfigsResourceQuery::new(2, "orders".to_owned(), None),
            DescribeConfigsResourceQuery::new(4, "7".to_owned(), None),
        ],
        false,
        false,
    )
    .unwrap_or_else(|error| panic!("valid mixed plan: {error}"));
    DescribeConfigsMachine::new(
        OperationId::from_raw(12),
        Deadline::from_tick(deadline),
        plan,
    )
}

fn apply(
    machine: &mut DescribeConfigsMachine,
    input: DescribeConfigsInput,
) -> super::DescribeConfigsTransition {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("valid transition: {error}"))
}

fn assert_failure(
    transition: super::DescribeConfigsTransition,
    expected_kind: DescribeConfigsFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let Some(DescribeConfigsEffect::Complete {
        terminal: DescribeConfigsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
