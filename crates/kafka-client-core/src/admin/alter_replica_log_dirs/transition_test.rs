//! Grouped mutation, partial-result, and terminal-assignment scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AlterReplicaLogDirAssignment, AlterReplicaLogDirBrokerError, AlterReplicaLogDirOutcome,
    AlterReplicaLogDirResult, AlterReplicaLogDirsEffect, AlterReplicaLogDirsFailureKind,
    AlterReplicaLogDirsInput, AlterReplicaLogDirsMachine, AlterReplicaLogDirsMachineError,
    AlterReplicaLogDirsPlan, AlterReplicaLogDirsTerminal,
};

#[test]
fn first_appearance_groups_reuse_original_deadline_and_restore_caller_order() {
    let mut machine = machine(vec![
        assignment(9, "orders", 0),
        assignment(2, "audit", 1),
        assignment(9, "orders", 2),
    ]);
    let first = effect(
        &mut machine,
        AlterReplicaLogDirsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    assert_submit(first, 9, &[("orders", 0), ("orders", 2)]);
    machine
        .apply(AlterReplicaLogDirsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first: {error}"));
    let second = effect(
        &mut machine,
        AlterReplicaLogDirsInput::BrokerResponded {
            throttle_time_ms: 4,
            outcomes: vec![altered(9, "orders", 0), altered(9, "orders", 2)],
        },
    );
    assert_submit(second, 2, &[("audit", 1)]);
    machine
        .apply(AlterReplicaLogDirsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second: {error}"));
    let terminal = effect(
        &mut machine,
        AlterReplicaLogDirsInput::BrokerResponded {
            throttle_time_ms: 11,
            outcomes: vec![broker_failed(2, "audit", 1, -17)],
        },
    );
    let AlterReplicaLogDirsEffect::Complete {
        operation_id,
        terminal: AlterReplicaLogDirsTerminal::Altered(batch),
    } = terminal
    else {
        panic!("expected altered terminal");
    };
    assert_eq!(operation_id, OperationId::from_raw(29));
    assert_eq!(batch.throttle_time_ms(), 11);
    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(|outcome| (outcome.broker_id(), outcome.topic(), outcome.partition()))
            .collect::<Vec<_>>(),
        vec![(9, "orders", 0), (2, "audit", 1), (9, "orders", 2)]
    );
    assert!(matches!(
        batch.outcomes()[1].result(),
        AlterReplicaLogDirResult::BrokerFailed(error) if error.code() == -17
    ));
}

#[test]
fn later_transport_failure_preserves_interleaved_success_and_marks_future_group_unattempted() {
    let mut machine = machine(vec![
        assignment(9, "orders", 0),
        assignment(2, "audit", 1),
        assignment(9, "orders", 2),
        assignment(4, "later", 3),
    ]);
    effect(
        &mut machine,
        AlterReplicaLogDirsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(AlterReplicaLogDirsInput::DriverAccepted)
        .and_then(|_| {
            machine.apply(AlterReplicaLogDirsInput::BrokerResponded {
                throttle_time_ms: 7,
                outcomes: vec![altered(9, "orders", 0), altered(9, "orders", 2)],
            })
        })
        .and_then(|_| machine.apply(AlterReplicaLogDirsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("complete first and accept second: {error}"));
    let terminal = effect(
        &mut machine,
        AlterReplicaLogDirsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        },
    );
    let AlterReplicaLogDirsEffect::Complete {
        terminal: AlterReplicaLogDirsTerminal::Altered(batch),
        ..
    } = terminal
    else {
        panic!("expected partial terminal");
    };
    assert_eq!(batch.throttle_time_ms(), 7);
    assert!(matches!(
        batch.outcomes()[0].result(),
        AlterReplicaLogDirResult::Altered
    ));
    let AlterReplicaLogDirResult::OperationFailed(current) = batch.outcomes()[1].result() else {
        panic!("current failure missing");
    };
    assert_eq!(current.kind(), AlterReplicaLogDirsFailureKind::Transport);
    assert_eq!(current.delivery(), DeliveryStatus::PossiblySent);
    assert!(matches!(
        batch.outcomes()[2].result(),
        AlterReplicaLogDirResult::Altered
    ));
    let AlterReplicaLogDirResult::OperationFailed(later) = batch.outcomes()[3].result() else {
        panic!("unattempted failure missing");
    };
    assert_eq!(later.kind(), AlterReplicaLogDirsFailureKind::NotAttempted);
    assert_eq!(later.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn mismatched_group_response_settles_invalid_once_without_retry() {
    let mut machine = machine(vec![assignment(7, "orders", 0)]);
    effect(
        &mut machine,
        AlterReplicaLogDirsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(AlterReplicaLogDirsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let terminal = effect(
        &mut machine,
        AlterReplicaLogDirsInput::BrokerResponded {
            throttle_time_ms: 0,
            outcomes: vec![altered(7, "orders", 1)],
        },
    );
    let AlterReplicaLogDirsEffect::Complete {
        terminal: AlterReplicaLogDirsTerminal::Altered(batch),
        ..
    } = terminal
    else {
        panic!("expected invalid terminal");
    };
    let AlterReplicaLogDirResult::OperationFailed(failure) = batch.outcomes()[0].result() else {
        panic!("invalid-response failure missing");
    };
    assert_eq!(
        failure.kind(),
        AlterReplicaLogDirsFailureKind::InvalidResponse
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    assert_eq!(
        machine.apply(AlterReplicaLogDirsInput::InvalidResponse),
        Err(AlterReplicaLogDirsMachineError::AlreadyCompleted)
    );
}

fn machine(assignments: Vec<AlterReplicaLogDirAssignment>) -> AlterReplicaLogDirsMachine {
    AlterReplicaLogDirsMachine::new(
        OperationId::from_raw(29),
        Deadline::from_tick(100),
        AlterReplicaLogDirsPlan::new(assignments)
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn assignment(broker_id: i32, topic: &str, partition: i32) -> AlterReplicaLogDirAssignment {
    AlterReplicaLogDirAssignment::new(
        broker_id,
        topic.to_owned(),
        partition,
        format!("/data/{broker_id}/{partition}"),
    )
}

fn altered(broker_id: i32, topic: &str, partition: i32) -> AlterReplicaLogDirOutcome {
    AlterReplicaLogDirOutcome::altered(broker_id, topic.to_owned(), partition)
}

fn broker_failed(
    broker_id: i32,
    topic: &str,
    partition: i32,
    code: i16,
) -> AlterReplicaLogDirOutcome {
    AlterReplicaLogDirOutcome::broker_failed(
        broker_id,
        topic.to_owned(),
        partition,
        AlterReplicaLogDirBrokerError::new(
            NonZeroI16::new(code).unwrap_or_else(|| panic!("nonzero")),
        ),
    )
}

fn effect(
    machine: &mut AlterReplicaLogDirsMachine,
    input: AlterReplicaLogDirsInput,
) -> AlterReplicaLogDirsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("expected effect"))
}

fn assert_submit(effect: AlterReplicaLogDirsEffect, broker_id: i32, expected: &[(&str, i32)]) {
    let AlterReplicaLogDirsEffect::Submit {
        operation_id,
        deadline,
        broker_id: actual,
        assignments,
    } = effect
    else {
        panic!("expected submit");
    };
    assert_eq!(operation_id, OperationId::from_raw(29));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(actual, broker_id);
    assert_eq!(
        assignments
            .iter()
            .map(|assignment| (assignment.topic(), assignment.partition()))
            .collect::<Vec<_>>(),
        expected
    );
}
