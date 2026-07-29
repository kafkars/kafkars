//! Admission, neutral submission, shutdown, and reclamation scenarios.

use std::sync::Arc;

use kafka_client_core::DescribeTopicPartitionsPlan;

use crate::{
    admin::{AdminCompletionNotifier, AdminDescribeTopicPartitionsHost},
    clock::MonotonicClock,
};

use super::{
    AdminDescribeTopicPartitionsAdmissionErrorKind, AdminDescribeTopicPartitionsDeliveryStatus,
    AdminDescribeTopicPartitionsFailureKind, AdminDescribeTopicPartitionsOutcome,
    AdminDescribeTopicPartitionsTurn, host::ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_full_envelope_before_neutral_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeTopicPartitionsHost::new(ports.describe_topic_partitions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit page: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan()),
        Err(AdminDescribeTopicPartitionsAdmissionErrorKind::RetainedBytes)
    ));

    let AdminDescribeTopicPartitionsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.topics(), ["orders", "audit"]);
    assert!(result_limit > ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES / 2);
    assert!(result_limit < ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES);

    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_is_definitely_unsent_and_reclaimable() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeTopicPartitionsHost::new(ports.describe_topic_partitions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit page: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched page: {error}"));
    let AdminDescribeTopicPartitionsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        AdminDescribeTopicPartitionsFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        AdminDescribeTopicPartitionsDeliveryStatus::NotSent
    );
    let _progress = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("reclaim: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn handed_off_shutdown_is_conservatively_possibly_sent() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeTopicPartitionsHost::new(ports.describe_topic_partitions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit page: {error:?}"));
    let AdminDescribeTopicPartitionsTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff: {error}"))
    else {
        panic!("submission expected");
    };

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover handoff: {error}"));
    let AdminDescribeTopicPartitionsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        AdminDescribeTopicPartitionsFailureKind::Transport
    );
    assert_eq!(
        failure.delivery(),
        AdminDescribeTopicPartitionsDeliveryStatus::PossiblySent
    );
    drop(host);
    stop_notifier(&mut notifier);
}

fn plan() -> DescribeTopicPartitionsPlan {
    DescribeTopicPartitionsPlan::new(vec!["orders".to_owned(), "audit".to_owned()], 2_000, None)
        .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn deadline() -> crate::clock::DeadlineCapture {
    Arc::new(MonotonicClock::new())
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"))
}

fn stop_notifier(notifier: &mut AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
