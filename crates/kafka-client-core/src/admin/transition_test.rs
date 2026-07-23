//! Scenarios for `CreateTopics` lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    CreateTopicBrokerError, CreateTopicOutcome, CreateTopicSpecification, CreateTopicsEffect,
    CreateTopicsFailureKind, CreateTopicsInput, CreateTopicsMachine, CreateTopicsMachineError,
    CreateTopicsPlan, CreateTopicsState, CreateTopicsTerminal,
};

fn machine(deadline: u64) -> CreateTopicsMachine {
    let plan = CreateTopicsPlan::new(
        vec![
            CreateTopicSpecification::new("orders", 3, 2, Vec::new()),
            CreateTopicSpecification::new("audit", 1, -1, Vec::new()),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid CreateTopics test plan: {error}"));
    CreateTopicsMachine::new(
        OperationId::from_raw(7),
        Deadline::from_tick(deadline),
        plan,
    )
}

fn start_and_accept(machine: &mut CreateTopicsMachine) {
    assert!(
        machine
            .apply(CreateTopicsInput::Start {
                now: Moment::from_tick(1),
            })
            .is_ok()
    );
    assert!(machine.apply(CreateTopicsInput::DriverAccepted).is_ok());
}

#[test]
fn start_emits_the_original_ordered_plan_and_deadline() {
    let mut machine = machine(50);
    let transition = machine.apply(CreateTopicsInput::Start {
        now: Moment::from_tick(10),
    });
    assert!(transition.is_ok());
    let Ok(transition) = transition else {
        return;
    };

    let Some(CreateTopicsEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = transition.effect()
    else {
        unreachable!();
    };
    assert_eq!(*operation_id, OperationId::from_raw(7));
    assert_eq!(*deadline, Deadline::from_tick(50));
    assert_eq!(plan.topics()[0].name(), "orders");
    assert_eq!(plan.topics()[1].name(), "audit");
    assert_eq!(machine.state(), CreateTopicsState::AwaitingDriver);
}

#[test]
fn mixed_broker_results_are_terminal_once_in_request_order() {
    let mut machine = machine(50);
    start_and_accept(&mut machine);
    let Some(code) = NonZeroI16::new(-32_000) else {
        return;
    };
    let outcomes = vec![
        CreateTopicOutcome::created("orders"),
        CreateTopicOutcome::failed(
            "audit",
            CreateTopicBrokerError::new(code, Some("future broker code".to_owned())),
        ),
    ];

    let terminal = machine.apply(CreateTopicsInput::BrokerResponded { outcomes });
    assert!(terminal.is_ok());
    let Ok(terminal) = terminal else {
        return;
    };
    let Some(CreateTopicsEffect::Complete { terminal, .. }) = terminal.effect() else {
        unreachable!();
    };
    let CreateTopicsTerminal::Topics(outcomes) = terminal else {
        unreachable!();
    };
    assert_eq!(outcomes[0].topic(), "orders");
    assert_eq!(outcomes[1].topic(), "audit");
    assert_eq!(machine.state(), CreateTopicsState::Completed);
    assert_eq!(
        machine.apply(CreateTopicsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        }),
        Err(CreateTopicsMachineError::AlreadyCompleted)
    );
}

#[test]
fn malformed_normalized_order_does_not_consume_terminal_assignment() {
    let mut machine = machine(50);
    start_and_accept(&mut machine);

    assert_eq!(
        machine.apply(CreateTopicsInput::BrokerResponded {
            outcomes: vec![
                CreateTopicOutcome::created("audit"),
                CreateTopicOutcome::created("orders"),
            ],
        }),
        Err(CreateTopicsMachineError::OutcomeTopicMismatch)
    );
    assert_eq!(machine.state(), CreateTopicsState::Submitted);
    assert!(
        machine
            .apply(CreateTopicsInput::BrokerResponded {
                outcomes: vec![
                    CreateTopicOutcome::created("orders"),
                    CreateTopicOutcome::created("audit"),
                ],
            })
            .is_ok()
    );
}

#[test]
fn elapsed_and_driver_rejected_operations_are_definitely_not_sent() {
    let mut elapsed = machine(10);
    let transition = elapsed.apply(CreateTopicsInput::Start {
        now: Moment::from_tick(10),
    });
    assert!(transition.is_ok());
    assert_failed(
        transition
            .ok()
            .and_then(super::CreateTopicsTransition::into_effect),
        CreateTopicsFailureKind::DeadlineElapsed,
    );

    let mut rejected = machine(50);
    assert!(
        rejected
            .apply(CreateTopicsInput::Start {
                now: Moment::from_tick(1),
            })
            .is_ok()
    );
    let transition = rejected.apply(CreateTopicsInput::DriverRejected);
    assert!(transition.is_ok());
    assert_failed(
        transition
            .ok()
            .and_then(super::CreateTopicsTransition::into_effect),
        CreateTopicsFailureKind::DriverRejected,
    );
}

fn assert_failed(effect: Option<CreateTopicsEffect>, expected: CreateTopicsFailureKind) {
    let Some(CreateTopicsEffect::Complete { terminal, .. }) = effect else {
        unreachable!();
    };
    let CreateTopicsTerminal::Failed(failure) = terminal else {
        unreachable!();
    };
    assert_eq!(failure.kind(), expected);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}
