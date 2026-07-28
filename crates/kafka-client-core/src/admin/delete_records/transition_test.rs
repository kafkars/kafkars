//! Transition scenarios for deterministic Admin `DeleteRecords` policy.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DeleteRecordsEffect, DeleteRecordsInput, DeleteRecordsMachine, DeleteRecordsOutcome,
    DeleteRecordsPlan, DeleteRecordsResult, DeleteRecordsTarget, DeleteRecordsTerminal,
    DeletedRecords,
};

#[test]
fn multiple_targets_execute_sequentially_and_preserve_caller_order() {
    let mut machine = machine(vec![target("orders", 2, 91), target("audit", 0, -1)]);
    assert!(matches!(
        machine
            .apply(DeleteRecordsInput::Start {
                now: Moment::from_tick(1)
            })
            .unwrap_or_else(|error| panic!("start: {error}"))
            .into_effect(),
        Some(DeleteRecordsEffect::Submit { target, .. }) if target.topic() == "orders"
    ));
    machine
        .apply(DeleteRecordsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let next = machine
        .apply(DeleteRecordsInput::BrokerResponded {
            throttle_time_ms: 4,
            outcome: DeleteRecordsOutcome::deleted("orders".to_owned(), 2, DeletedRecords::new(91)),
        })
        .unwrap_or_else(|error| panic!("response: {error}"));
    assert!(matches!(
        next.into_effect(),
        Some(DeleteRecordsEffect::Submit { target, .. }) if target.topic() == "audit"
    ));
    machine
        .apply(DeleteRecordsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let terminal = machine
        .apply(DeleteRecordsInput::BrokerResponded {
            throttle_time_ms: 9,
            outcome: DeleteRecordsOutcome::deleted("audit".to_owned(), 0, DeletedRecords::new(42)),
        })
        .unwrap_or_else(|error| panic!("response: {error}"));
    let Some(DeleteRecordsEffect::Complete {
        terminal: DeleteRecordsTerminal::Deleted(batch),
        ..
    }) = terminal.into_effect()
    else {
        panic!("expected deletion terminal");
    };
    assert_eq!(batch.throttle_time_ms(), 9);
    assert!(matches!(
        batch.outcomes()[0].result(),
        DeleteRecordsResult::Deleted(_)
    ));
    assert_eq!(batch.outcomes()[1].topic(), "audit");
}

#[test]
fn completed_transport_failure_and_unattempted_target_are_retained() {
    let mut machine = machine(vec![
        target("a", 0, 91),
        target("b", 1, 42),
        target("c", 2, 17),
    ]);
    machine
        .apply(DeleteRecordsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    machine
        .apply(DeleteRecordsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    machine
        .apply(DeleteRecordsInput::BrokerResponded {
            throttle_time_ms: 11,
            outcome: DeleteRecordsOutcome::deleted("a".to_owned(), 0, DeletedRecords::new(91)),
        })
        .unwrap_or_else(|error| panic!("response: {error}"));
    machine
        .apply(DeleteRecordsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second: {error}"));
    let terminal = machine
        .apply(DeleteRecordsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("transport: {error}"));
    let Some(DeleteRecordsEffect::Complete {
        terminal: DeleteRecordsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("expected failure terminal");
    };
    assert_eq!(failure.throttle_time_ms(), 11);
    assert_eq!(failure.completed().len(), 1);
    assert_eq!(failure.completed()[0].topic(), "a");
    assert_eq!(failure.failed_target().topic(), "b");
    assert_eq!(failure.failed_target().partition(), 1);
    assert_eq!(failure.unattempted().len(), 1);
    assert_eq!(failure.unattempted()[0].topic(), "c");
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn completed_pre_driver_deadline_keeps_current_target_not_sent() {
    let mut machine = machine(vec![target("a", 0, 91), target("b", 1, 42)]);
    machine
        .apply(DeleteRecordsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DeleteRecordsInput::DriverAccepted))
        .and_then(|_| {
            machine.apply(DeleteRecordsInput::BrokerResponded {
                throttle_time_ms: 3,
                outcome: DeleteRecordsOutcome::deleted("a".to_owned(), 0, DeletedRecords::new(91)),
            })
        })
        .unwrap_or_else(|error| panic!("complete first target: {error}"));
    let terminal = machine
        .apply(DeleteRecordsInput::DeadlineElapsed)
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let Some(DeleteRecordsEffect::Complete {
        terminal: DeleteRecordsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("expected failure terminal");
    };
    assert_eq!(failure.completed()[0].topic(), "a");
    assert_eq!(failure.failed_target().topic(), "b");
    assert!(failure.unattempted().is_empty());
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

fn machine(targets: Vec<DeleteRecordsTarget>) -> DeleteRecordsMachine {
    DeleteRecordsMachine::new(
        OperationId::from_raw(7),
        Deadline::from_tick(99),
        DeleteRecordsPlan::new(targets).unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn target(topic: &str, partition: i32, offset: i64) -> DeleteRecordsTarget {
    DeleteRecordsTarget::new(topic.to_owned(), partition, offset)
}
