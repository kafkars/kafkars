//! Bounded `DescribeCluster` deadline, terminal, and byte-release scenarios.

use std::time::Instant;

use kafka_client_core::{DeliveryStatus, DescribeClusterInput};

use crate::clock::OperationDeadline;

use super::{
    DescribeClusterDeliveryStatus, DescribeClusterFailureKind, DescribeClusterHost,
    DescribeClusterOutcome, DescribeClusterTurn, test_support::describe_cluster_host,
    test_support::stop_notifier,
};

#[test]
fn reserved_result_bytes_survive_until_observer_reclamation() {
    let (mut host, notifier) = describe_cluster_host();
    let admission = host
        .try_admit(kafka_client_core::Moment::from_tick(1), deadline(10))
        .unwrap_or_else(|error| panic!("admit DescribeCluster: {error:?}"));
    let DescribeClusterTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must be ready");
    };
    let operation_id = submission.operation_id;
    host.apply(operation_id, DescribeClusterInput::DriverAccepted)
        .and_then(|()| {
            host.apply(
                operation_id,
                DescribeClusterInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                },
            )
        })
        .unwrap_or_else(|error| panic!("finish operation: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 128 * 1024);
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe terminal: {error}"));
    let DescribeClusterOutcome::Failed(failure) = outcome else {
        panic!("transport failure expected");
    };
    assert_eq!(failure.kind(), DescribeClusterFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        DescribeClusterDeliveryStatus::PossiblySent
    );
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    stop(host, notifier);
}

#[test]
fn pre_driver_deadline_is_definitely_unsent() {
    let (mut host, notifier) = describe_cluster_host();
    let admission = host
        .try_admit(kafka_client_core::Moment::from_tick(1), deadline(2))
        .unwrap_or_else(|error| panic!("admit DescribeCluster: {error:?}"));
    assert!(matches!(
        host.turn(kafka_client_core::Moment::from_tick(2)),
        Ok(DescribeClusterTurn::Progress)
    ));
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe terminal: {error}"));
    let DescribeClusterOutcome::Failed(failure) = outcome else {
        panic!("deadline failure expected");
    };
    assert_eq!(failure.kind(), DescribeClusterFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery(), DescribeClusterDeliveryStatus::NotSent);
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    stop(host, notifier);
}

#[test]
fn optional_response_expansions_cross_the_existing_submission_owner() {
    let (mut host, notifier) = describe_cluster_host();
    let admission = host
        .try_admit_with_options(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            true,
            true,
        )
        .unwrap_or_else(|error| panic!("admit DescribeCluster: {error:?}"));
    let DescribeClusterTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must be ready");
    };
    assert!(submission.include_fenced_brokers);
    assert!(submission.include_authorized_operations);

    host.apply(
        submission.operation_id,
        DescribeClusterInput::DriverRejected,
    )
    .unwrap_or_else(|error| panic!("reject submission: {error}"));
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe terminal: {error}"));
    let DescribeClusterOutcome::Failed(failure) = outcome else {
        panic!("driver rejection expected");
    };
    assert_eq!(failure.kind(), DescribeClusterFailureKind::DriverRejected);
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    stop(host, notifier);
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}

fn stop(host: DescribeClusterHost, notifier: super::completion::AdminCompletionNotifier) {
    drop(host);
    stop_notifier(notifier);
}
