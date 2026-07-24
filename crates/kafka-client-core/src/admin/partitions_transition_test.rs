//! Scenarios for `CreatePartitions` terminal single assignment.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    CreatePartitionsEffect, CreatePartitionsInput, CreatePartitionsMachine,
    CreatePartitionsMachineError, CreatePartitionsPlan, CreatePartitionsSpecification,
    CreatePartitionsState, CreatePartitionsTerminal, PartitionIncreaseOutcome,
};

fn machine(deadline: u64) -> CreatePartitionsMachine {
    let plan = CreatePartitionsPlan::new(
        vec![
            CreatePartitionsSpecification::new("orders".to_owned(), 8),
            CreatePartitionsSpecification::new("audit".to_owned(), 4),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid CreatePartitions test plan: {error}"));
    CreatePartitionsMachine::new(
        OperationId::from_raw(11),
        Deadline::from_tick(deadline),
        plan,
    )
}

#[test]
fn ordered_terminal_is_single_assignment() {
    let mut machine = machine(20);
    let started = machine
        .apply(CreatePartitionsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert!(matches!(
        started.into_effect(),
        Some(CreatePartitionsEffect::Submit { .. })
    ));
    machine
        .apply(CreatePartitionsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    let terminal = machine
        .apply(CreatePartitionsInput::BrokerResponded {
            outcomes: vec![
                PartitionIncreaseOutcome::increased("orders"),
                PartitionIncreaseOutcome::increased("audit"),
            ],
        })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    assert!(matches!(
        terminal.into_effect(),
        Some(CreatePartitionsEffect::Complete {
            terminal: CreatePartitionsTerminal::Topics(_),
            ..
        })
    ));
    assert_eq!(machine.state(), CreatePartitionsState::Completed);
    assert_eq!(
        machine.apply(CreatePartitionsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        }),
        Err(CreatePartitionsMachineError::AlreadyCompleted)
    );
}

#[test]
fn deadline_and_response_order_remain_core_owned() {
    let mut elapsed = machine(10);
    let terminal = elapsed
        .apply(CreatePartitionsInput::Start {
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("elapsed start settles: {error}"));
    assert!(matches!(
        terminal.into_effect(),
        Some(CreatePartitionsEffect::Complete {
            terminal: CreatePartitionsTerminal::Failed(_),
            ..
        })
    ));

    let mut mismatch = machine(20);
    mismatch
        .apply(CreatePartitionsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| mismatch.apply(CreatePartitionsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("setup should succeed: {error}"));
    assert_eq!(
        mismatch.apply(CreatePartitionsInput::BrokerResponded {
            outcomes: vec![
                PartitionIncreaseOutcome::increased("audit"),
                PartitionIncreaseOutcome::increased("orders"),
            ],
        }),
        Err(CreatePartitionsMachineError::OutcomeTopicMismatch)
    );
    assert_eq!(mismatch.state(), CreatePartitionsState::Submitted);
}
