//! Broker grouping, correlation, caller ordering, and partial-failure scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeReplicaLogDirsBrokerError, DescribeReplicaLogDirsEffect,
    DescribeReplicaLogDirsFailureKind, DescribeReplicaLogDirsInput, DescribeReplicaLogDirsMachine,
    DescribeReplicaLogDirsPlan, DescribeReplicaLogDirsReplica,
    DescribeReplicaLogDirsReplicaOutcome, DescribeReplicaLogDirsReplicaPlacement,
    DescribeReplicaLogDirsReplicaResult, DescribeReplicaLogDirsTerminal, ReplicaLogDirInfo,
    ReplicaLogDirLocation,
};

#[test]
fn broker_groups_run_in_first_occurrence_order_and_restore_caller_order() {
    let mut machine = machine();
    assert_submit(start(&mut machine), 8, &[("orders", 0), ("orders", 1)]);
    accept(&mut machine);
    assert_submit(
        effect(
            &mut machine,
            DescribeReplicaLogDirsInput::BrokerResponded {
                broker_id: 8,
                throttle_time_ms: 4,
                result: Ok(vec![
                    placement("orders", 0, 8, Some(("/logs/a", 0)), None),
                    placement("orders", 1, 8, None, Some(("/logs/b", 7))),
                ]),
            },
        ),
        3,
        &[("audit", 0)],
    );
    accept(&mut machine);
    let terminal = effect(
        &mut machine,
        DescribeReplicaLogDirsInput::BrokerResponded {
            broker_id: 3,
            throttle_time_ms: 11,
            result: Ok(vec![placement("audit", 0, 3, None, None)]),
        },
    );
    let DescribeReplicaLogDirsEffect::Complete {
        terminal: DescribeReplicaLogDirsTerminal::Described(batch),
        ..
    } = terminal
    else {
        panic!("described terminal expected");
    };
    assert_eq!(batch.throttle_time_ms(), 11);
    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(DescribeReplicaLogDirsReplicaOutcome::replica)
            .map(|replica| (replica.topic(), replica.partition(), replica.broker_id()))
            .collect::<Vec<_>>(),
        vec![("orders", 0, 8), ("audit", 0, 3), ("orders", 1, 8)]
    );
    let DescribeReplicaLogDirsReplicaResult::Described(info) = batch.outcomes()[1].result() else {
        panic!("missing placement success expected");
    };
    assert!(info.current().is_none());
    assert!(info.future().is_none());
}

#[test]
fn top_level_broker_error_maps_to_every_requested_replica_and_continues() {
    let mut machine = machine();
    start(&mut machine);
    accept(&mut machine);
    let error = DescribeReplicaLogDirsBrokerError::new(
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
    );
    assert_submit(
        effect(
            &mut machine,
            DescribeReplicaLogDirsInput::BrokerResponded {
                broker_id: 8,
                throttle_time_ms: 5,
                result: Err(error),
            },
        ),
        3,
        &[("audit", 0)],
    );
    accept(&mut machine);
    let batch = completed_batch(effect(
        &mut machine,
        DescribeReplicaLogDirsInput::BrokerResponded {
            broker_id: 3,
            throttle_time_ms: 1,
            result: Ok(vec![placement("audit", 0, 3, None, None)]),
        },
    ));
    for index in [0, 2] {
        assert!(matches!(
            batch.outcomes()[index].result(),
            DescribeReplicaLogDirsReplicaResult::BrokerFailed(actual)
                if actual.code() == -32_000
        ));
    }
}

#[test]
fn current_mechanism_failure_preserves_delivery_and_marks_later_brokers_unattempted() {
    let mut machine = machine();
    start(&mut machine);
    accept(&mut machine);
    let batch = completed_batch(effect(
        &mut machine,
        DescribeReplicaLogDirsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        },
    ));
    for index in [0, 2] {
        let DescribeReplicaLogDirsReplicaResult::OperationFailed(failure) =
            batch.outcomes()[index].result()
        else {
            panic!("current broker failure expected");
        };
        assert_eq!(failure.kind(), DescribeReplicaLogDirsFailureKind::Transport);
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    }
    let DescribeReplicaLogDirsReplicaResult::OperationFailed(failure) =
        batch.outcomes()[1].result()
    else {
        panic!("later broker failure expected");
    };
    assert_eq!(
        failure.kind(),
        DescribeReplicaLogDirsFailureKind::NotAttempted
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn mismatched_or_missing_placement_settles_invalid_response_once() {
    let mut machine = machine();
    start(&mut machine);
    accept(&mut machine);
    let batch = completed_batch(effect(
        &mut machine,
        DescribeReplicaLogDirsInput::BrokerResponded {
            broker_id: 8,
            throttle_time_ms: 0,
            result: Ok(vec![placement("orders", 1, 8, None, None)]),
        },
    ));
    let DescribeReplicaLogDirsReplicaResult::OperationFailed(failure) =
        batch.outcomes()[0].result()
    else {
        panic!("invalid response expected");
    };
    assert_eq!(
        failure.kind(),
        DescribeReplicaLogDirsFailureKind::InvalidResponse
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

fn machine() -> DescribeReplicaLogDirsMachine {
    DescribeReplicaLogDirsMachine::new(
        OperationId::from_raw(35),
        Deadline::from_tick(100),
        DescribeReplicaLogDirsPlan::new(vec![
            replica("orders", 0, 8),
            replica("audit", 0, 3),
            replica("orders", 1, 8),
        ])
        .unwrap_or_else(|error| panic!("plan: {error}")),
    )
}

fn replica(topic: &str, partition: i32, broker_id: i32) -> DescribeReplicaLogDirsReplica {
    DescribeReplicaLogDirsReplica::new(topic.to_owned(), partition, broker_id)
}

fn placement(
    topic: &str,
    partition: i32,
    broker_id: i32,
    current: Option<(&str, i64)>,
    future: Option<(&str, i64)>,
) -> DescribeReplicaLogDirsReplicaPlacement {
    DescribeReplicaLogDirsReplicaPlacement::new(
        replica(topic, partition, broker_id),
        ReplicaLogDirInfo::new(
            current.map(|(path, lag)| ReplicaLogDirLocation::new(path.to_owned(), lag)),
            future.map(|(path, lag)| ReplicaLogDirLocation::new(path.to_owned(), lag)),
        ),
    )
}

fn start(machine: &mut DescribeReplicaLogDirsMachine) -> DescribeReplicaLogDirsEffect {
    effect(
        machine,
        DescribeReplicaLogDirsInput::Start {
            now: Moment::from_tick(1),
        },
    )
}

fn accept(machine: &mut DescribeReplicaLogDirsMachine) {
    machine
        .apply(DescribeReplicaLogDirsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accepted: {error}"));
}

fn effect(
    machine: &mut DescribeReplicaLogDirsMachine,
    input: DescribeReplicaLogDirsInput,
) -> DescribeReplicaLogDirsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect expected"))
}

fn assert_submit(effect: DescribeReplicaLogDirsEffect, broker_id: i32, identities: &[(&str, i32)]) {
    let DescribeReplicaLogDirsEffect::Submit {
        operation_id,
        deadline,
        broker_id: actual,
        replicas,
    } = effect
    else {
        panic!("submit expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(35));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(actual, broker_id);
    assert_eq!(
        replicas
            .iter()
            .map(|replica| (replica.topic(), replica.partition()))
            .collect::<Vec<_>>(),
        identities
    );
}

fn completed_batch(effect: DescribeReplicaLogDirsEffect) -> super::DescribeReplicaLogDirsBatch {
    let DescribeReplicaLogDirsEffect::Complete {
        terminal: DescribeReplicaLogDirsTerminal::Described(batch),
        ..
    } = effect
    else {
        panic!("complete expected");
    };
    batch
}
