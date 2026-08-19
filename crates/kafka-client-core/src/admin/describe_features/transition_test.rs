//! Submission, completion, deadline, and delivery-certainty scenarios.

#![expect(
    clippy::needless_pass_by_value,
    reason = "test helpers preserve exact effect ownership"
)]

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeFeaturesBrokerError, DescribeFeaturesDescription, DescribeFeaturesEffect,
    DescribeFeaturesFailureKind, DescribeFeaturesInput, DescribeFeaturesMachine,
    DescribeFeaturesMachineError, DescribeFeaturesState, DescribeFeaturesTerminal,
};

#[test]
fn one_fixed_request_reuses_the_original_public_deadline() {
    let mut machine = machine();
    let effect = effect(
        &mut machine,
        DescribeFeaturesInput::Start {
            now: Moment::from_tick(2),
        },
    );
    assert!(matches!(
        effect,
        DescribeFeaturesEffect::Submit {
            operation_id,
            deadline,
        } if operation_id == OperationId::from_raw(57)
            && deadline == Deadline::from_tick(1_000)
    ));
    assert_eq!(machine.state(), DescribeFeaturesState::AwaitingDriver);
    accept(&mut machine);
    assert_eq!(machine.state(), DescribeFeaturesState::Submitted);
}

#[test]
fn validated_description_becomes_the_sole_terminal() {
    let mut machine = submitted_machine();
    let description = DescribeFeaturesDescription::new(5, vec![], true, Some(9), vec![], true)
        .unwrap_or_else(|error| panic!("description: {error}"));
    let terminal = effect(
        &mut machine,
        DescribeFeaturesInput::BrokerResponded {
            description: description.clone(),
        },
    );
    assert!(matches!(
        terminal,
        DescribeFeaturesEffect::Complete {
            operation_id,
            terminal: DescribeFeaturesTerminal::Described(actual),
        } if operation_id == OperationId::from_raw(57) && actual == description
    ));
    assert_eq!(machine.state(), DescribeFeaturesState::Completed);
    assert_eq!(
        machine.apply(DescribeFeaturesInput::InvalidResponse),
        Err(DescribeFeaturesMachineError::AlreadyCompleted)
    );
}

#[test]
fn exact_broker_rejection_is_not_reclassified() {
    let mut machine = submitted_machine();
    let error = DescribeFeaturesBrokerError::new(
        11,
        NonZeroI16::new(-17).unwrap_or_else(|| panic!("nonzero code")),
    );
    let terminal = effect(
        &mut machine,
        DescribeFeaturesInput::BrokerRejected { error },
    );
    assert!(matches!(
        terminal,
        DescribeFeaturesEffect::Complete {
            operation_id,
            terminal: DescribeFeaturesTerminal::BrokerRejected(actual),
        } if operation_id == OperationId::from_raw(57) && actual == error
    ));
}

#[test]
fn pre_driver_expiry_and_rejection_are_definitely_not_sent() {
    let mut expired = machine();
    assert_failure(
        effect(
            &mut expired,
            DescribeFeaturesInput::Start {
                now: Moment::from_tick(1_000),
            },
        ),
        DescribeFeaturesFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine();
    let _ = effect(
        &mut rejected,
        DescribeFeaturesInput::Start {
            now: Moment::from_tick(1),
        },
    );
    assert_failure(
        effect(&mut rejected, DescribeFeaturesInput::DriverRejected),
        DescribeFeaturesFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn submitted_failures_preserve_authoritative_delivery() {
    for (input, kind, delivery) in [
        (
            DescribeFeaturesInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DescribeFeaturesFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeFeaturesInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeFeaturesFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            DescribeFeaturesInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeFeaturesFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            DescribeFeaturesInput::ResponseTooLarge,
            DescribeFeaturesFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeFeaturesInput::InvalidResponse,
            DescribeFeaturesFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        assert_failure(effect(&mut submitted_machine(), input), kind, delivery);
    }
}

#[test]
fn facts_cannot_skip_or_repeat_lifecycle_stages() {
    let mut ready = machine();
    assert_eq!(
        ready.apply(DescribeFeaturesInput::DriverAccepted),
        Err(DescribeFeaturesMachineError::InvalidState)
    );
    let _ = effect(
        &mut ready,
        DescribeFeaturesInput::Start {
            now: Moment::from_tick(1),
        },
    );
    assert_eq!(
        ready.apply(DescribeFeaturesInput::Start {
            now: Moment::from_tick(2),
        }),
        Err(DescribeFeaturesMachineError::InvalidState)
    );
}

fn machine() -> DescribeFeaturesMachine {
    DescribeFeaturesMachine::new(OperationId::from_raw(57), Deadline::from_tick(1_000))
}

fn submitted_machine() -> DescribeFeaturesMachine {
    let mut machine = machine();
    let _ = effect(
        &mut machine,
        DescribeFeaturesInput::Start {
            now: Moment::from_tick(1),
        },
    );
    accept(&mut machine);
    machine
}

fn accept(machine: &mut DescribeFeaturesMachine) {
    let transition = machine
        .apply(DescribeFeaturesInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    assert!(transition.into_effect().is_none());
}

fn effect(
    machine: &mut DescribeFeaturesMachine,
    input: DescribeFeaturesInput,
) -> DescribeFeaturesEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn assert_failure(
    effect: DescribeFeaturesEffect,
    expected_kind: DescribeFeaturesFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let DescribeFeaturesEffect::Complete {
        terminal: DescribeFeaturesTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
