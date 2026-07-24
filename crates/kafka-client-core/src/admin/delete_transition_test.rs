//! Scenarios for `DeleteTopics` lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

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
