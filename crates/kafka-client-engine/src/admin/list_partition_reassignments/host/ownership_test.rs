//! Synchronous rejection ownership and exact-correlation scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{ListPartitionReassignmentTarget, ListPartitionReassignmentsPlan, Moment};

use crate::{
    admin::{
        AdminCompletionNotifier, ListPartitionReassignmentsDeliveryStatus,
        ListPartitionReassignmentsFailureKind, ListPartitionReassignmentsOutcome,
    },
    clock::OperationDeadline,
};

use super::{
    ListPartitionReassignmentsHost, ListPartitionReassignmentsHostError,
    ListPartitionReassignmentsTurn,
};

#[test]
fn synchronous_rejection_preserves_exact_not_sent_settlement() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, ListPartitionReassignmentsPlan::all_active());
    let ListPartitionReassignmentsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, plan, result_limit) = submission.into_parts();

    host.reject_handoff(operation_id, plan, result_limit)
        .unwrap_or_else(|error| panic!("reject exact submission: {error}"));
    let ListPartitionReassignmentsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe rejection: {error}"))
    else {
        panic!("driver rejection expected");
    };
    assert_eq!(
        failure.into_parts(),
        (
            ListPartitionReassignmentsFailureKind::DriverRejected,
            ListPartitionReassignmentsDeliveryStatus::NotSent,
        )
    );

    drop(host);
    stop_notifier(notifier);
}

#[test]
fn mismatched_partition_filter_retains_rejection_and_blocks_publication() {
    let (mut host, notifier) = host();
    let expected = selected_plan(0);
    let admission = admit(&mut host, expected);
    let ListPartitionReassignmentsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, _plan, result_limit) = submission.into_parts();

    assert!(matches!(
        host.reject_handoff(operation_id, selected_plan(1), result_limit),
        Err(ListPartitionReassignmentsHostError::SubmissionMismatch)
    ));
    assert!(host.rejected_submission_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(ListPartitionReassignmentsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

fn admit(
    host: &mut ListPartitionReassignmentsHost,
    plan: ListPartitionReassignmentsPlan,
) -> super::ListPartitionReassignmentsAdmission {
    host.try_admit(Moment::from_tick(1), deadline(), plan)
        .unwrap_or_else(|error| panic!("admit listing: {error:?}"))
}

fn selected_plan(partition: i32) -> ListPartitionReassignmentsPlan {
    ListPartitionReassignmentsPlan::selected(vec![ListPartitionReassignmentTarget::new(
        "orders".to_owned(),
        partition,
    )])
    .unwrap_or_else(|error| panic!("valid selected plan: {error}"))
}

fn host() -> (ListPartitionReassignmentsHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("start notifier: {error}"));
    (
        ListPartitionReassignmentsHost::new(ports.list_partition_reassignments),
        notifier,
    )
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    )
}

fn stop_notifier(mut notifier: AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
