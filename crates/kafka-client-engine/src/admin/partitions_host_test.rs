//! Bounded `CreatePartitions` deadline, recovery, and byte-release scenarios.

use std::time::Instant;

use kafka_client_core::{
    CreatePartitionsInput, CreatePartitionsPlan, CreatePartitionsSpecification, DeliveryStatus,
};

use crate::clock::OperationDeadline;

use super::{
    CreatePartitionsDeliveryStatus, CreatePartitionsFailureKind, CreatePartitionsHost,
    CreatePartitionsOutcome, CreatePartitionsTurn, test_support::create_partitions_host,
    test_support::stop_notifier,
};

fn plan() -> CreatePartitionsPlan {
    CreatePartitionsPlan::new(
        vec![CreatePartitionsSpecification::new("orders".to_owned(), 8)],
        false,
    )
    .unwrap_or_else(|error| panic!("valid partition plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}

#[test]
fn terminal_bytes_remain_reserved_until_observer_reclamation() {
    let (mut host, notifier) = create_partitions_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit partitions: {error:?}"));
    let CreatePartitionsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must be ready");
    };
    host.apply(
        submission.operation_id,
        CreatePartitionsInput::DriverAccepted,
    )
    .and_then(|()| {
        host.apply(
            submission.operation_id,
            CreatePartitionsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )
    })
    .unwrap_or_else(|error| panic!("settle partitions: {error}"));
    assert_eq!(host.retained_bytes, 16 * 1024);
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe terminal: {error}"));
    assert!(matches!(
        outcome,
        CreatePartitionsOutcome::Failed(failure)
            if failure.kind() == CreatePartitionsFailureKind::Transport
                && failure.delivery() == CreatePartitionsDeliveryStatus::PossiblySent
    ));
    let _turn = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    assert_eq!(host.retained_bytes, 0);
    drop(host);
    stop_notifier(notifier);
}

#[test]
fn driver_deadline_translates_without_erasing_possibly_sent_certainty() {
    let (mut host, notifier) = create_partitions_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit partitions: {error:?}"));
    let CreatePartitionsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must be ready");
    };
    host.apply(
        submission.operation_id,
        CreatePartitionsInput::DriverAccepted,
    )
    .and_then(|()| {
        host.apply(
            submission.operation_id,
            CreatePartitionsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )
    })
    .unwrap_or_else(|error| panic!("settle driver deadline: {error}"));
    let CreatePartitionsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe deadline: {error}"))
    else {
        panic!("whole-operation deadline failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            CreatePartitionsFailureKind::DeadlineElapsed,
            CreatePartitionsDeliveryStatus::PossiblySent,
        )
    );
    let _turn = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim deadline: {error}"));
    stop(host, notifier);
}

#[test]
fn queued_and_taken_recovery_preserve_driver_ownership_boundary() {
    for taken in [false, true] {
        let (mut host, notifier) = create_partitions_host();
        let admission = host
            .try_admit(
                kafka_client_core::Moment::from_tick(1),
                deadline(10),
                plan(),
                16 * 1024,
            )
            .unwrap_or_else(|error| panic!("admit partitions: {error:?}"));
        if taken {
            assert!(matches!(
                host.turn(kafka_client_core::Moment::from_tick(2)),
                Ok(CreatePartitionsTurn::Submit(_))
            ));
        }
        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover partitions: {error}"));
        let CreatePartitionsOutcome::Failed(failure) = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe recovery: {error}"))
        else {
            panic!("whole-operation failure expected");
        };
        let expected = if taken {
            (
                CreatePartitionsFailureKind::Transport,
                CreatePartitionsDeliveryStatus::PossiblySent,
            )
        } else {
            (
                CreatePartitionsFailureKind::DriverRejected,
                CreatePartitionsDeliveryStatus::NotSent,
            )
        };
        assert_eq!((failure.kind(), failure.delivery()), expected);
        let _turn = host
            .turn(kafka_client_core::Moment::from_tick(3))
            .unwrap_or_else(|error| panic!("reclaim result: {error}"));
        stop(host, notifier);
    }
}

fn stop(host: CreatePartitionsHost, notifier: super::completion::AdminCompletionNotifier) {
    drop(host);
    stop_notifier(notifier);
}
