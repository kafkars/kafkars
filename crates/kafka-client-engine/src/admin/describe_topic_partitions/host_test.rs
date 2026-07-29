//! Admission, neutral submission, shutdown, and reclamation scenarios.

use std::sync::Arc;

use kafka_client_core::{DescribeTopicPartitionsMachineError, DescribeTopicPartitionsPlan, Moment};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, AdminDescribeTopicPartitionsHost},
    clock::MonotonicClock,
    driver::{DescribeTopicPartitionsCall, DriverOwner},
    protocol::admin::describe_topic_partitions::{
        DescribeTopicPartitionsRequestCursor, DescribeTopicPartitionsRequestPlan,
        describe_topic_partitions_request,
    },
};

use super::{
    AdminDescribeTopicPartitionsAdmissionErrorKind, AdminDescribeTopicPartitionsDeliveryStatus,
    AdminDescribeTopicPartitionsFailureKind, AdminDescribeTopicPartitionsHostError,
    AdminDescribeTopicPartitionsOutcome, AdminDescribeTopicPartitionsTurn,
    host::ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES,
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
    let (operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.topics(), ["orders", "audit"]);
    assert!(result_limit > ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES / 2);
    assert!(result_limit < ADMIN_DESCRIBE_TOPIC_PARTITIONS_RETAINED_BYTES);

    drop(admission.observer);
    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("reject handoff: {error}"));
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
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
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

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AdminDescribeTopicPartitionsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_survives_core_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeTopicPartitionsHost::new(ports.describe_topic_partitions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit page: {error:?}"));
    let AdminDescribeTopicPartitionsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff: {error}"))
    else {
        panic!("submission expected");
    };
    drop(submission);
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(AdminDescribeTopicPartitionsHostError::Machine(
            DescribeTopicPartitionsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_ownership_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AdminDescribeTopicPartitionsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeTopicPartitionsHost::new(ports.describe_topic_partitions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit page: {error:?}"));
    let AdminDescribeTopicPartitionsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, route_plan, retained_limit) = submission.into_parts();
    let cursor = route_plan.cursor().map(|cursor| {
        DescribeTopicPartitionsRequestCursor::new(cursor.topic_name(), cursor.partition_index())
    });
    let request = describe_topic_partitions_request(
        DescribeTopicPartitionsRequestPlan::new(
            route_plan.topics(),
            route_plan.response_partition_limit(),
            cursor,
        ),
        retained_limit,
    )
    .unwrap_or_else(|error| panic!("request: {error:?}"));
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call =
        DescribeTopicPartitionsCall::submit(&driver, request, submitted_deadline.transport())
            .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(capture.now().tick().saturating_add(1))),
        Err(AdminDescribeTopicPartitionsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AdminDescribeTopicPartitionsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AdminDescribeTopicPartitionsFailureKind::Transport,
            AdminDescribeTopicPartitionsDeliveryStatus::PossiblySent,
        )
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
