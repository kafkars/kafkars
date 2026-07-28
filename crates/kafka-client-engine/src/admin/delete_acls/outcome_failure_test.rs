//! Admin `DeleteAcls` mechanism-failure and delivery translation tests.

use kafka_client_core::{
    Deadline, DeleteAclsEffect as CoreEffect, DeleteAclsFilter as CoreFilter,
    DeleteAclsInput as CoreInput, DeleteAclsMachine as CoreMachine, DeleteAclsPlan as CorePlan,
    DeleteAclsTerminal as CoreTerminal, DeliveryStatus, Moment, OperationId,
};

use super::{
    DeleteAclsBatch, DeleteAclsDeliveryStatus, DeleteAclsFailure, DeleteAclsFailureKind,
    DeleteAclsOutcome, outcome::translate_terminal_into,
};

#[test]
fn every_mechanism_failure_and_delivery_certainty_is_translated() {
    for (input, submitted, expected_kind, expected_delivery) in [
        (
            CoreInput::DeadlineElapsed,
            false,
            DeleteAclsFailureKind::DeadlineElapsed,
            DeleteAclsDeliveryStatus::NotSent,
        ),
        (
            CoreInput::DriverRejected,
            false,
            DeleteAclsFailureKind::DriverRejected,
            DeleteAclsDeliveryStatus::NotSent,
        ),
        (
            CoreInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            true,
            DeleteAclsFailureKind::DeadlineElapsed,
            DeleteAclsDeliveryStatus::PossiblySent,
        ),
        (
            CoreInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            true,
            DeleteAclsFailureKind::Transport,
            DeleteAclsDeliveryStatus::PossiblySent,
        ),
        (
            CoreInput::ResponseTooLarge,
            true,
            DeleteAclsFailureKind::ResponseTooLarge,
            DeleteAclsDeliveryStatus::PossiblySent,
        ),
        (
            CoreInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            true,
            DeleteAclsFailureKind::Compatibility,
            DeleteAclsDeliveryStatus::NotSent,
        ),
        (
            CoreInput::InvalidResponse,
            true,
            DeleteAclsFailureKind::InvalidResponse,
            DeleteAclsDeliveryStatus::PossiblySent,
        ),
    ] {
        let failure = translate_failure(input, submitted);
        assert_eq!(failure.kind(), expected_kind);
        assert_eq!(failure.delivery(), expected_delivery);
    }
}

fn translate_failure(input: CoreInput, submitted: bool) -> DeleteAclsFailure {
    let mut machine = machine();
    let _ = machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    if submitted {
        machine
            .apply(CoreInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accept driver call: {error}"));
    }
    let effect = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("complete machine: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("terminal expected"));
    let prepared = DeleteAclsBatch::try_prepare_outcomes(1)
        .unwrap_or_else(|error| panic!("reserve outcome: {error}"));
    let DeleteAclsOutcome::Failed(failure) = translate_terminal_into(terminal(effect), prepared)
        .unwrap_or_else(|failure| panic!("translate failure: {:?}", failure.error()))
    else {
        panic!("failure expected");
    };
    failure
}

fn machine() -> CoreMachine {
    CoreMachine::new(
        OperationId::from_raw(43),
        Deadline::from_tick(100),
        CorePlan::new(vec![CoreFilter::new(1, None, 1, None, None, 1, 1)])
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn terminal(effect: CoreEffect) -> CoreTerminal {
    let CoreEffect::Complete { terminal, .. } = effect else {
        panic!("completion expected");
    };
    terminal
}
