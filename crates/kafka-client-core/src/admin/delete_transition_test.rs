//! Scenarios for `DeleteTopics` lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::delete_outcome::DeleteTopicIdOutcome;
use super::{
    DeleteTopicBrokerError, DeleteTopicOutcome, DeleteTopicsEffect, DeleteTopicsInput,
    DeleteTopicsMachine, DeleteTopicsMachineError, DeleteTopicsPlan, DeleteTopicsState,
    DeleteTopicsTerminal,
};

fn machine(deadline: u64) -> DeleteTopicsMachine {
    let plan = DeleteTopicsPlan::new(vec!["orders".to_owned(), "audit".to_owned()])
        .unwrap_or_else(|error| panic!("valid DeleteTopics test plan: {error}"));
    DeleteTopicsMachine::new(
        OperationId::from_raw(7),
        Deadline::from_tick(deadline),
        plan,
    )
}

#[test]
fn ordered_terminal_is_single_assignment_and_lossless() {
    let mut machine = machine(20);
    let started = machine
        .apply(DeleteTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert!(matches!(
        started.into_effect(),
        Some(DeleteTopicsEffect::Submit { .. })
    ));
    machine
        .apply(DeleteTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    let code = NonZeroI16::new(-321).unwrap_or_else(|| panic!("code is nonzero"));
    let outcomes = vec![
        DeleteTopicOutcome::deleted("orders"),
        DeleteTopicOutcome::failed(
            "audit",
            DeleteTopicBrokerError::with_bounded_message(code, Some("unknown".to_owned()), false),
        ),
    ];
    let terminal = machine
        .apply(DeleteTopicsInput::BrokerResponded { outcomes })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    assert!(matches!(
        terminal.into_effect(),
        Some(DeleteTopicsEffect::Complete {
            terminal: DeleteTopicsTerminal::Topics(_),
            ..
        })
    ));
    assert_eq!(machine.state(), DeleteTopicsState::Completed);
    assert_eq!(
        machine.apply(DeleteTopicsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        }),
        Err(DeleteTopicsMachineError::AlreadyCompleted)
    );
}

#[test]
fn deadline_and_response_order_are_owned_by_core() {
    let mut elapsed = machine(10);
    assert!(matches!(
        elapsed
            .apply(DeleteTopicsInput::Start {
                now: Moment::from_tick(10),
            })
            .and_then(DeleteTopicsTransitionExt::effect),
        Ok(DeleteTopicsTerminal::Failed(_))
    ));

    let mut mismatch = machine(20);
    mismatch
        .apply(DeleteTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| mismatch.apply(DeleteTopicsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("setup should succeed: {error}"));
    assert_eq!(
        mismatch.apply(DeleteTopicsInput::BrokerResponded {
            outcomes: vec![
                DeleteTopicOutcome::deleted("audit"),
                DeleteTopicOutcome::deleted("orders"),
            ],
        }),
        Err(DeleteTopicsMachineError::OutcomeTopicMismatch)
    );
    assert_eq!(mismatch.state(), DeleteTopicsState::Submitted);
}

#[test]
fn topic_id_terminal_preserves_caller_order_and_identity_kind() {
    let first = [1; 16];
    let second = [2; 16];
    let plan = DeleteTopicsPlan::by_ids(vec![first, second])
        .unwrap_or_else(|error| panic!("valid topic-ID deletion plan: {error}"));
    let mut machine =
        DeleteTopicsMachine::new(OperationId::from_raw(9), Deadline::from_tick(20), plan);
    machine
        .apply(DeleteTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DeleteTopicsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("topic-ID setup should succeed: {error}"));
    let terminal = machine
        .apply(DeleteTopicsInput::BrokerRespondedById {
            outcomes: vec![
                DeleteTopicIdOutcome::deleted(first),
                DeleteTopicIdOutcome::deleted(second),
            ],
        })
        .unwrap_or_else(|error| panic!("topic-ID response should settle: {error}"));
    assert!(matches!(
        terminal.into_effect(),
        Some(DeleteTopicsEffect::Complete {
            terminal: DeleteTopicsTerminal::TopicIds(outcomes),
            ..
        }) if outcomes.iter().map(DeleteTopicIdOutcome::topic_id).collect::<Vec<_>>()
            == vec![first, second]
    ));

    let plan = DeleteTopicsPlan::by_ids(vec![first, second])
        .unwrap_or_else(|error| panic!("valid topic-ID deletion plan: {error}"));
    let mut mismatch =
        DeleteTopicsMachine::new(OperationId::from_raw(10), Deadline::from_tick(20), plan);
    mismatch
        .apply(DeleteTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| mismatch.apply(DeleteTopicsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("topic-ID setup should succeed: {error}"));
    assert_eq!(
        mismatch.apply(DeleteTopicsInput::BrokerRespondedById {
            outcomes: vec![
                DeleteTopicIdOutcome::deleted(second),
                DeleteTopicIdOutcome::deleted(first),
            ],
        }),
        Err(DeleteTopicsMachineError::OutcomeTopicIdMismatch)
    );
}

struct DeleteTopicsTransitionExt;

impl DeleteTopicsTransitionExt {
    fn effect(
        transition: super::DeleteTopicsTransition,
    ) -> Result<DeleteTopicsTerminal, DeleteTopicsMachineError> {
        let Some(DeleteTopicsEffect::Complete { terminal, .. }) = transition.into_effect() else {
            return Err(DeleteTopicsMachineError::InvalidState);
        };
        Ok(terminal)
    }
}
