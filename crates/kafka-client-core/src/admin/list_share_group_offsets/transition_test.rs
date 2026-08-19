//! Lifecycle, deadline, delivery, and terminal scenarios for API 90.

#![expect(
    clippy::needless_pass_by_value,
    reason = "test helpers preserve exact terminal ownership"
)]

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ListShareGroupOffsetTarget, ListShareGroupOffsetsBrokerError, ListShareGroupOffsetsEffect,
    ListShareGroupOffsetsFailureKind, ListShareGroupOffsetsInput, ListShareGroupOffsetsMachine,
    ListShareGroupOffsetsMachineError, ListShareGroupOffsetsPlan, ListShareGroupOffsetsState,
    ListShareGroupOffsetsTerminal,
};

#[test]
fn start_emits_one_exact_submission_or_pre_driver_deadline_terminal() {
    let plan = selected_plan();
    let mut owner = machine(20, plan.clone());
    let effect = apply(
        &mut owner,
        ListShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    assert_eq!(
        effect,
        ListShareGroupOffsetsEffect::Submit {
            operation_id: OperationId::from_raw(90),
            deadline: Deadline::from_tick(20),
            plan,
        }
    );
    assert_eq!(owner.state(), ListShareGroupOffsetsState::AwaitingDriver);

    let mut elapsed = machine(10, all_plan());
    let terminal = terminal(apply(
        &mut elapsed,
        ListShareGroupOffsetsInput::Start {
            now: Moment::from_tick(10),
        },
    ));
    assert_failure(
        terminal,
        ListShareGroupOffsetsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn exact_group_rejection_and_mechanism_delivery_are_distinct() {
    let error = ListShareGroupOffsetsBrokerError::new(
        17,
        nonzero(-32_000),
        Some("group rejected".to_owned()),
        false,
    );
    let mut machine = submitted(all_plan());
    assert_eq!(
        terminal(apply(
            &mut machine,
            ListShareGroupOffsetsInput::BrokerRejected {
                error: error.clone(),
            },
        )),
        ListShareGroupOffsetsTerminal::BrokerRejected(error)
    );

    for (input, kind, delivery) in [
        (
            ListShareGroupOffsetsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            ListShareGroupOffsetsFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            ListShareGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            ListShareGroupOffsetsFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            ListShareGroupOffsetsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            ListShareGroupOffsetsFailureKind::Transport,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = submitted(all_plan());
        assert_failure(terminal(apply(&mut machine, input)), kind, delivery);
    }
}

#[test]
fn awaiting_driver_rejection_is_definitely_unsent() {
    let mut machine = machine(20, all_plan());
    let _submit = apply(
        &mut machine,
        ListShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        },
    );

    assert_failure(
        terminal(apply(
            &mut machine,
            ListShareGroupOffsetsInput::DriverRejected,
        )),
        ListShareGroupOffsetsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn completed_machine_rejects_every_later_fact() {
    let mut machine = submitted(all_plan());
    let _terminal = apply(&mut machine, ListShareGroupOffsetsInput::InvalidResponse);

    assert_eq!(
        machine.apply(ListShareGroupOffsetsInput::DriverRejected),
        Err(ListShareGroupOffsetsMachineError::AlreadyCompleted)
    );
}

fn selected_plan() -> ListShareGroupOffsetsPlan {
    ListShareGroupOffsetsPlan::selected(
        "share-workers".to_owned(),
        vec![
            ListShareGroupOffsetTarget::new("orders".to_owned(), 2),
            ListShareGroupOffsetTarget::new("audit".to_owned(), 0),
            ListShareGroupOffsetTarget::new("orders".to_owned(), 1),
        ],
    )
    .unwrap_or_else(|error| panic!("selected plan: {error}"))
}

fn all_plan() -> ListShareGroupOffsetsPlan {
    ListShareGroupOffsetsPlan::all("share-workers".to_owned())
        .unwrap_or_else(|error| panic!("all plan: {error}"))
}

fn machine(deadline: u64, plan: ListShareGroupOffsetsPlan) -> ListShareGroupOffsetsMachine {
    ListShareGroupOffsetsMachine::new(
        OperationId::from_raw(90),
        Deadline::from_tick(deadline),
        plan,
    )
}

fn submitted(plan: ListShareGroupOffsetsPlan) -> ListShareGroupOffsetsMachine {
    let mut machine = machine(20, plan);
    let _submit = apply(
        &mut machine,
        ListShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    let transition = machine
        .apply(ListShareGroupOffsetsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept driver: {error}"));
    assert!(transition.into_effect().is_none());
    machine
}

fn apply(
    machine: &mut ListShareGroupOffsetsMachine,
    input: ListShareGroupOffsetsInput,
) -> ListShareGroupOffsetsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("apply input: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect expected"))
}

fn terminal(effect: ListShareGroupOffsetsEffect) -> ListShareGroupOffsetsTerminal {
    let ListShareGroupOffsetsEffect::Complete { terminal, .. } = effect else {
        panic!("terminal effect expected");
    };
    terminal
}

fn assert_failure(
    terminal: ListShareGroupOffsetsTerminal,
    kind: ListShareGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
) {
    let ListShareGroupOffsetsTerminal::Failed(failure) = terminal else {
        panic!("mechanism failure expected");
    };
    assert_eq!((failure.kind(), failure.delivery()), (kind, delivery));
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}
