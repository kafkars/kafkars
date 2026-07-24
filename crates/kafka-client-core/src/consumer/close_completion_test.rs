//! Exact direct-consumer close acceptance and terminal-completion scenarios.

use super::{
    AssignedConsumerCloseId, AssignedConsumerEffect, AssignedConsumerInput,
    AssignedConsumerMachine, AssignedConsumerMachineError, StartPosition,
    assignment_test::{assign, assigned, offset},
};
use crate::{Deadline, Moment};

#[test]
fn only_exact_drain_proof_selects_the_close_terminal_once() {
    let mut machine = AssignedConsumerMachine::new();
    assign(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(7)))],
    );
    let accepted = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("begin close: {error}"));
    let close_id = accepted_close(accepted.effects());
    let wrong = AssignedConsumerCloseId::from_raw_for_test(close_id.get() + 1);

    assert_eq!(
        machine.apply(AssignedConsumerInput::CloseDrained { close_id: wrong }),
        Err(AssignedConsumerMachineError::StaleClose {
            active: close_id,
            supplied: wrong,
        })
    );
    let complete = machine
        .apply(AssignedConsumerInput::CloseDrained { close_id })
        .unwrap_or_else(|error| panic!("complete exact close: {error}"));
    assert_eq!(
        complete.effects(),
        &[AssignedConsumerEffect::CompleteClose { close_id }]
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::CloseDrained { close_id }),
        Err(AssignedConsumerMachineError::CloseAlreadyCompleted { close_id })
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::BeginClose),
        Err(AssignedConsumerMachineError::ConsumerClosed)
    );
}

#[test]
fn unassigned_close_permanently_closes_and_completes_without_assignment_identity() {
    let mut machine = AssignedConsumerMachine::new();
    let accepted = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("begin unassigned close: {error}"));
    assert_eq!(accepted.assignment_epoch(), None);
    let close_id = accepted_close(accepted.effects());
    assert_eq!(accepted.effects().len(), 1);
    assert!(machine.is_closed());
    assert_eq!(
        machine.apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(4, 0, StartPosition::Offset(offset(1)))],
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        }),
        Err(AssignedConsumerMachineError::ConsumerClosed)
    );

    let complete = machine
        .apply(AssignedConsumerInput::CloseDrained { close_id })
        .unwrap_or_else(|error| panic!("complete unassigned close: {error}"));
    assert_eq!(complete.assignment_epoch(), None);
    assert_eq!(
        complete.effects(),
        &[AssignedConsumerEffect::CompleteClose { close_id }]
    );
}

#[test]
fn drain_before_acceptance_is_rejected_without_closing_admission() {
    let mut machine = AssignedConsumerMachine::new();
    let supplied = AssignedConsumerCloseId::from_raw_for_test(7);
    assert_eq!(
        machine.apply(AssignedConsumerInput::CloseDrained { close_id: supplied }),
        Err(AssignedConsumerMachineError::CloseNotPending { supplied })
    );
    assert!(!machine.is_closed());
}

fn accepted_close(effects: &[AssignedConsumerEffect]) -> AssignedConsumerCloseId {
    let Some(AssignedConsumerEffect::AcceptClose { close_id }) = effects.first() else {
        panic!("first effect must accept close");
    };
    *close_id
}
