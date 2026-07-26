//! Bounded `DescribeTopics` admission, deadline, terminal, and byte-release scenarios.

use std::time::Instant;

use kafka_client_core::{DeliveryStatus, DescribeTopicsInput, DescribeTopicsPlan};

use crate::clock::OperationDeadline;

use super::{
    DescribeTopicsDeliveryStatus, DescribeTopicsFailureKind, DescribeTopicsHost,
    DescribeTopicsOutcome, DescribeTopicsTurn, limits::DESCRIBE_TOPICS_RETAINED_BYTES,
};
use crate::admin::test_support::{describe_topics_host, stop_notifier};

fn plan() -> DescribeTopicsPlan {
    DescribeTopicsPlan::new(vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("valid DescribeTopics plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}

#[test]
fn terminal_bytes_remain_reserved_until_observer_reclamation() {
    let (mut host, notifier) = describe_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit DescribeTopics: {error:?}"));
    let DescribeTopicsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must be ready");
    };
    host.apply(submission.operation_id, DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("apply driver acceptance: {error}"));
    host.apply(
        submission.operation_id,
        DescribeTopicsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        },
    )
    .unwrap_or_else(|error| panic!("apply terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 16 * 1024);

    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe terminal: {error}"));
    let DescribeTopicsOutcome::Failed(failure) = outcome else {
        panic!("transport failure expected");
    };
    assert_eq!(failure.kind(), DescribeTopicsFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        DescribeTopicsDeliveryStatus::PossiblySent
    );
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    stop(host, notifier);
}

#[test]
fn pre_driver_wait_expires_without_transport_ownership() {
    let (mut host, notifier) = describe_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(2),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit DescribeTopics: {error:?}"));
    assert!(matches!(
        host.turn(kafka_client_core::Moment::from_tick(2)),
        Ok(DescribeTopicsTurn::Progress)
    ));
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe deadline terminal: {error}"));
    let DescribeTopicsOutcome::Failed(failure) = outcome else {
        panic!("deadline failure expected");
    };
    assert_eq!(failure.kind(), DescribeTopicsFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery(), DescribeTopicsDeliveryStatus::NotSent);
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    stop(host, notifier);
}

#[test]
fn already_elapsed_start_publishes_its_terminal_without_submission() {
    let (mut host, notifier) = describe_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(2),
            deadline(2),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit elapsed DescribeTopics: {error:?}"));
    assert_eq!(host.unsettled(), 0);
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"));
    assert!(matches!(
        outcome,
        DescribeTopicsOutcome::Failed(failure)
            if failure.kind() == DescribeTopicsFailureKind::DeadlineElapsed
    ));
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim elapsed terminal: {error}"));
    stop(host, notifier);
}

#[test]
fn recovery_after_submission_handoff_is_conservatively_possibly_sent() {
    let (mut host, notifier) = describe_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit DescribeTopics: {error:?}"));
    let DescribeTopicsTurn::Submit(_submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must cross the handoff boundary");
    };

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover handed-off submission: {error}"));
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery terminal: {error}"));
    let DescribeTopicsOutcome::Failed(failure) = outcome else {
        panic!("transport failure expected");
    };
    assert_eq!(failure.kind(), DescribeTopicsFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        DescribeTopicsDeliveryStatus::PossiblySent
    );
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim recovery terminal: {error}"));
    stop(host, notifier);
}

#[test]
fn recovery_of_untouched_queued_submission_remains_not_sent() {
    let (mut host, notifier) = describe_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit DescribeTopics: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover queued submission: {error}"));
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery terminal: {error}"));
    let DescribeTopicsOutcome::Failed(failure) = outcome else {
        panic!("driver rejection expected");
    };
    assert_eq!(failure.kind(), DescribeTopicsFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), DescribeTopicsDeliveryStatus::NotSent);
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim recovery terminal: {error}"));
    stop(host, notifier);
}

#[test]
fn retained_byte_limit_rejects_before_completion_reservation() {
    let (mut host, notifier) = describe_topics_host();
    let rejection = host.try_admit(
        kafka_client_core::Moment::from_tick(1),
        deadline(10),
        plan(),
        DESCRIBE_TOPICS_RETAINED_BYTES + 1,
    );
    assert!(matches!(
        rejection,
        Err(super::DescribeTopicsAdmissionErrorKind::RetainedBytes)
    ));
    stop(host, notifier);
}

#[test]
fn all_topic_reservation_backpressures_another_operation_until_reclaimed() {
    let (mut host, notifier) = describe_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            DescribeTopicsPlan::all(false),
            DESCRIBE_TOPICS_RETAINED_BYTES,
        )
        .unwrap_or_else(|error| panic!("admit bounded all-topic query: {error:?}"));
    assert!(matches!(
        host.try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            1,
        ),
        Err(super::DescribeTopicsAdmissionErrorKind::RetainedBytes)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover queued all-topic query: {error}"));
    let _outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovered query: {error}"));
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("reclaim all-topic bytes: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    stop(host, notifier);
}

fn stop(host: DescribeTopicsHost, notifier: crate::admin::completion::AdminCompletionNotifier) {
    drop(host);
    stop_notifier(notifier);
}
