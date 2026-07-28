//! Deadline, submission, failure-certainty, and terminal lifecycle scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeUserScramCredentialsBrokerError, DescribeUserScramCredentialsEffect,
    DescribeUserScramCredentialsFailure, DescribeUserScramCredentialsFailureKind,
    DescribeUserScramCredentialsInput, DescribeUserScramCredentialsMachine,
    DescribeUserScramCredentialsMachineError, DescribeUserScramCredentialsPlan,
    DescribeUserScramCredentialsTerminal,
};

#[test]
fn one_submission_reuses_the_original_deadline_and_plan() {
    let mut machine = machine();
    let submit = effect(
        &mut machine,
        DescribeUserScramCredentialsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    let DescribeUserScramCredentialsEffect::Submit {
        operation_id,
        deadline,
        plan,
    } = submit
    else {
        panic!("submit expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(50));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(
        plan.users().unwrap_or_else(|| panic!("named users")),
        ["zed", "alice"]
    );

    assert!(
        machine
            .apply(DescribeUserScramCredentialsInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accept: {error}"))
            .into_effect()
            .is_none()
    );
}

#[test]
fn elapsed_original_deadline_is_definitely_unsent() {
    let terminal = effect(
        &mut machine(),
        DescribeUserScramCredentialsInput::Start {
            now: Moment::from_tick(100),
        },
    );
    let failure = failure(terminal);

    assert_eq!(
        failure.kind(),
        &DescribeUserScramCredentialsFailureKind::DeadlineElapsed
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn broker_and_transport_failures_preserve_exact_delivery_facts() {
    let broker_terminal = effect(
        &mut submitted_machine(),
        DescribeUserScramCredentialsInput::BrokerRejected {
            error: DescribeUserScramCredentialsBrokerError::new(
                NonZeroI16::new(-29).unwrap_or_else(|| panic!("nonzero")),
                Some("denied".to_owned()),
                false,
            ),
        },
    );
    let broker_failure = failure(broker_terminal);
    let DescribeUserScramCredentialsFailureKind::Broker(error) = broker_failure.kind() else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), -29);
    assert_eq!(broker_failure.delivery(), DeliveryStatus::PossiblySent);

    let transport_failure = failure(effect(
        &mut submitted_machine(),
        DescribeUserScramCredentialsInput::TransportFailed {
            delivery: DeliveryStatus::NotSent,
        },
    ));
    assert_eq!(
        transport_failure.kind(),
        &DescribeUserScramCredentialsFailureKind::Transport
    );
    assert_eq!(transport_failure.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn facts_are_stage_fenced_and_completion_is_terminal() {
    assert_eq!(
        machine().apply(DescribeUserScramCredentialsInput::DriverAccepted),
        Err(DescribeUserScramCredentialsMachineError::InvalidState)
    );

    let mut completed = machine();
    effect(
        &mut completed,
        DescribeUserScramCredentialsInput::Start {
            now: Moment::from_tick(100),
        },
    );
    assert_eq!(
        completed.apply(DescribeUserScramCredentialsInput::InvalidResponse),
        Err(DescribeUserScramCredentialsMachineError::AlreadyCompleted)
    );
}

fn machine() -> DescribeUserScramCredentialsMachine {
    DescribeUserScramCredentialsMachine::new(
        OperationId::from_raw(50),
        Deadline::from_tick(100),
        DescribeUserScramCredentialsPlan::new(Some(vec!["zed".to_owned(), "alice".to_owned()]))
            .unwrap_or_else(|error| panic!("valid selection: {error}")),
    )
}

fn submitted_machine() -> DescribeUserScramCredentialsMachine {
    let mut machine = machine();
    effect(
        &mut machine,
        DescribeUserScramCredentialsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(DescribeUserScramCredentialsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    machine
}

fn effect(
    machine: &mut DescribeUserScramCredentialsMachine,
    input: DescribeUserScramCredentialsInput,
) -> DescribeUserScramCredentialsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect expected"))
}

fn failure(effect: DescribeUserScramCredentialsEffect) -> DescribeUserScramCredentialsFailure {
    let DescribeUserScramCredentialsEffect::Complete {
        terminal: DescribeUserScramCredentialsTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("failed terminal expected");
    };
    failure
}
