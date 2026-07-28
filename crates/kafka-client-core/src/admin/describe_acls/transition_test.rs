//! Deadline, ordering, exact-error, and terminal-assignment scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeAclBinding, DescribeAclsBatch, DescribeAclsBrokerError, DescribeAclsEffect,
    DescribeAclsFailureKind, DescribeAclsFilter, DescribeAclsInput, DescribeAclsMachine,
    DescribeAclsMachineError, DescribeAclsPlan, DescribeAclsTerminal,
};

#[test]
fn one_submission_reuses_deadline_and_sorts_bindings_deterministically() {
    let mut machine = machine();
    let submit = effect(
        &mut machine,
        DescribeAclsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    let DescribeAclsEffect::Submit {
        operation_id,
        deadline,
        plan,
    } = submit
    else {
        panic!("submit expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(37));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(plan.filter().resource_type(), 1);

    machine
        .apply(DescribeAclsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let terminal = effect(
        &mut machine,
        DescribeAclsInput::BrokerResponded {
            batch: DescribeAclsBatch::new(
                13,
                vec![
                    binding(3, "alpha", "User:z"),
                    binding(2, "orders", "User:b"),
                    binding(2, "orders", "User:a"),
                ],
            ),
        },
    );
    let DescribeAclsEffect::Complete {
        terminal: DescribeAclsTerminal::Described(batch),
        ..
    } = terminal
    else {
        panic!("described terminal expected");
    };
    assert_eq!(batch.throttle_time_ms(), 13);
    assert_eq!(
        batch
            .bindings()
            .iter()
            .map(|binding| (
                binding.resource_type(),
                binding.resource_name(),
                binding.principal()
            ))
            .collect::<Vec<_>>(),
        vec![
            (3, "alpha", "User:z"),
            (2, "orders", "User:a"),
            (2, "orders", "User:b"),
        ]
    );
    assert_eq!(
        machine.apply(DescribeAclsInput::InvalidResponse),
        Err(DescribeAclsMachineError::AlreadyCompleted)
    );
}

#[test]
fn broker_and_transport_failures_preserve_exact_delivery_facts() {
    let mut broker_machine = submitted_machine();
    let broker_terminal = effect(
        &mut broker_machine,
        DescribeAclsInput::BrokerRejected {
            error: DescribeAclsBrokerError::new(
                NonZeroI16::new(-17).unwrap_or_else(|| panic!("nonzero")),
                Some("denied".to_owned()),
                false,
            ),
        },
    );
    let broker_failure = failure(broker_terminal);
    let DescribeAclsFailureKind::Broker(error) = broker_failure.kind() else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), -17);
    assert_eq!(error.message(), Some("denied"));
    assert_eq!(broker_failure.delivery(), DeliveryStatus::PossiblySent);

    let mut transport_machine = submitted_machine();
    let transport_terminal = effect(
        &mut transport_machine,
        DescribeAclsInput::TransportFailed {
            delivery: DeliveryStatus::NotSent,
        },
    );
    let transport_failure = failure(transport_terminal);
    assert_eq!(
        transport_failure.kind(),
        &DescribeAclsFailureKind::Transport
    );
    assert_eq!(transport_failure.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn malformed_binding_settles_invalid_without_retry() {
    let mut machine = submitted_machine();
    let terminal = effect(
        &mut machine,
        DescribeAclsInput::BrokerResponded {
            batch: DescribeAclsBatch::new(
                0,
                vec![DescribeAclBinding::new(
                    1,
                    "orders".to_owned(),
                    3,
                    "User:a".to_owned(),
                    "*".to_owned(),
                    3,
                    3,
                )],
            ),
        },
    );
    let failure = failure(terminal);
    assert_eq!(failure.kind(), &DescribeAclsFailureKind::InvalidResponse);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn elapsed_original_deadline_is_definitely_unsent() {
    let mut machine = machine();
    let terminal = effect(
        &mut machine,
        DescribeAclsInput::Start {
            now: Moment::from_tick(100),
        },
    );
    let failure = failure(terminal);
    assert_eq!(failure.kind(), &DescribeAclsFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

fn machine() -> DescribeAclsMachine {
    DescribeAclsMachine::new(
        OperationId::from_raw(37),
        Deadline::from_tick(100),
        DescribeAclsPlan::new(DescribeAclsFilter::new(1, None, 1, None, None, 1, 1))
            .unwrap_or_else(|error| panic!("valid filter: {error}")),
    )
}

fn submitted_machine() -> DescribeAclsMachine {
    let mut machine = machine();
    effect(
        &mut machine,
        DescribeAclsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(DescribeAclsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    machine
}

fn binding(resource_type: i8, resource_name: &str, principal: &str) -> DescribeAclBinding {
    DescribeAclBinding::new(
        resource_type,
        resource_name.to_owned(),
        3,
        principal.to_owned(),
        "*".to_owned(),
        3,
        3,
    )
}

fn effect(machine: &mut DescribeAclsMachine, input: DescribeAclsInput) -> DescribeAclsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect expected"))
}

fn failure(effect: DescribeAclsEffect) -> super::DescribeAclsFailure {
    let DescribeAclsEffect::Complete {
        terminal: DescribeAclsTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("failed terminal expected");
    };
    failure
}
