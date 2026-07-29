//! Success and response-correlation scenarios for producer fencing.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminFenceProducerBrokerError, AdminFenceProducerOutcome, AdminFenceProducerResult,
    AdminFenceProducersEffect, AdminFenceProducersFailureKind, AdminFenceProducersInput,
    AdminFenceProducersMachine, AdminFenceProducersMachineError, AdminFenceProducersPlan,
    AdminFenceProducersState, AdminFenceProducersTerminal, AdminFenceProducersTransition,
    AdminFencedProducerIdentity,
};

#[test]
fn each_id_is_submitted_once_with_the_original_deadline_and_caller_order() {
    let mut machine = two_id_machine(20);
    assert_submit(start(&mut machine), "invoice-worker");
    machine
        .apply(AdminFenceProducersInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first: {error}"));
    let second = machine
        .apply(AdminFenceProducersInput::BrokerResponded {
            throttle_time_ms: 17,
            outcome: fenced("invoice-worker", 91, 7),
        })
        .unwrap_or_else(|error| panic!("first response: {error}"));
    assert_submit(second, "audit-writer");

    machine
        .apply(AdminFenceProducersInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second: {error}"));
    let code = NonZeroI16::new(-31_777).unwrap_or_else(|| panic!("nonzero code"));
    let completed = machine
        .apply(AdminFenceProducersInput::BrokerResponded {
            throttle_time_ms: 73,
            outcome: AdminFenceProducerOutcome::broker_failed(
                "audit-writer".to_owned(),
                AdminFenceProducerBrokerError::new(code),
            ),
        })
        .unwrap_or_else(|error| panic!("second response: {error}"));
    let Some(AdminFenceProducersEffect::Complete {
        terminal: AdminFenceProducersTerminal::Fenced(batch),
        ..
    }) = completed.into_effect()
    else {
        panic!("second response must complete");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].transactional_id(), "invoice-worker");
    let AdminFenceProducerResult::Fenced(identity) = batch.outcomes()[0].result() else {
        panic!("first ID must retain its producer identity");
    };
    assert_eq!(identity.into_parts(), (91, 7));
    let AdminFenceProducerResult::BrokerFailed(error) = batch.outcomes()[1].result() else {
        panic!("second ID must retain broker failure");
    };
    assert_eq!(error.code(), -31_777);
    assert_eq!(machine.state(), AdminFenceProducersState::Completed);
    assert_eq!(
        machine.apply(AdminFenceProducersInput::InvalidResponse),
        Err(AdminFenceProducersMachineError::AlreadyCompleted)
    );
}

#[test]
fn mismatched_identity_is_invalid_and_never_advances_to_another_submit() {
    let mut machine = one_id_machine(20);
    start(&mut machine);
    machine
        .apply(AdminFenceProducersInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept call: {error}"));
    let transition = machine
        .apply(AdminFenceProducersInput::BrokerResponded {
            throttle_time_ms: 0,
            outcome: fenced("other", 91, 7),
        })
        .unwrap_or_else(|error| panic!("mismatch settles: {error}"));
    assert_failure(
        transition,
        AdminFenceProducersFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn every_submitted_failure_category_is_terminal_without_a_retry_effect() {
    for (input, kind, delivery) in [
        (
            AdminFenceProducersInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            AdminFenceProducersFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            AdminFenceProducersInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            AdminFenceProducersFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            AdminFenceProducersInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            AdminFenceProducersFailureKind::Compatibility,
            DeliveryStatus::PossiblySent,
        ),
        (
            AdminFenceProducersInput::ResponseTooLarge,
            AdminFenceProducersFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            AdminFenceProducersInput::InvalidResponse,
            AdminFenceProducersFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = one_id_machine(20);
        start(&mut machine);
        machine
            .apply(AdminFenceProducersInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accept call: {error}"));
        let transition = machine
            .apply(input)
            .unwrap_or_else(|error| panic!("failure settles: {error}"));
        assert_failure(transition, kind, delivery);
    }
}

fn fenced(transactional_id: &str, producer_id: i64, epoch: i16) -> AdminFenceProducerOutcome {
    let identity = AdminFencedProducerIdentity::try_new(producer_id, epoch)
        .unwrap_or_else(|| panic!("valid producer identity"));
    AdminFenceProducerOutcome::fenced(transactional_id.to_owned(), identity)
}

fn two_id_machine(deadline: u64) -> AdminFenceProducersMachine {
    machine(
        deadline,
        vec!["invoice-worker".to_owned(), "audit-writer".to_owned()],
    )
}

fn one_id_machine(deadline: u64) -> AdminFenceProducersMachine {
    machine(deadline, vec!["invoice-worker".to_owned()])
}

fn machine(deadline: u64, ids: Vec<String>) -> AdminFenceProducersMachine {
    AdminFenceProducersMachine::new(
        OperationId::from_raw(31),
        Deadline::from_tick(deadline),
        AdminFenceProducersPlan::new(ids).unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn start(machine: &mut AdminFenceProducersMachine) -> AdminFenceProducersTransition {
    machine
        .apply(AdminFenceProducersInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"))
}

fn assert_submit(transition: AdminFenceProducersTransition, transactional_id: &str) {
    let Some(AdminFenceProducersEffect::Submit {
        operation_id,
        deadline,
        transactional_id: submitted_id,
    }) = transition.into_effect()
    else {
        panic!("expected submit effect");
    };
    assert_eq!(operation_id, OperationId::from_raw(31));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(submitted_id, transactional_id);
}

fn assert_failure(
    transition: AdminFenceProducersTransition,
    kind: AdminFenceProducersFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AdminFenceProducersEffect::Complete {
        terminal: AdminFenceProducersTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
