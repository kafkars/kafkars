//! Completion-error ownership remains installed until post-driver recovery.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AlterPartitionReassignment, AlterPartitionReassignmentsPlan, Deadline, Moment,
    PartitionReassignmentTarget,
};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, AlterPartitionReassignmentsHost},
    clock::OperationDeadline,
    driver::{AlterPartitionReassignmentsCall, DriverOwner},
};

use super::{
    AlterPartitionReassignmentsDeliveryStatus, AlterPartitionReassignmentsFailureKind,
    AlterPartitionReassignmentsHostError, AlterPartitionReassignmentsOutcome,
    AlterPartitionReassignmentsTurn,
};

#[test]
fn completion_fault_retains_call_for_post_driver_recovery() {
    let (mut host, notifier) = host();
    let deadline = deadline(10);
    let plan = AlterPartitionReassignmentsPlan::new(vec![AlterPartitionReassignment::new(
        "orders".to_owned(),
        0,
        PartitionReassignmentTarget::Replicas(vec![1, 2]),
    )])
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan)
        .unwrap_or_else(|error| panic!("admission: {error:?}"));
    let AlterPartitionReassignmentsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, request_scratch_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = AlterPartitionReassignmentsCall::submit(
        &driver,
        submitted_plan,
        request_scratch_limit,
        result_limit,
        submitted_deadline,
        Moment::from_tick(2),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(AlterPartitionReassignmentsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"));
    let AlterPartitionReassignmentsOutcome::Failed(failure) = outcome else {
        panic!("recovery failure expected");
    };
    let (kind, delivery) = failure.into_parts();
    assert_eq!(kind, AlterPartitionReassignmentsFailureKind::Transport);
    assert_eq!(
        delivery,
        AlterPartitionReassignmentsDeliveryStatus::PossiblySent
    );

    drop(host);
    stop_notifier(notifier);
}

#[test]
fn mismatched_rejection_evidence_blocks_settlement() {
    let (mut host, notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan("orders"))
        .unwrap_or_else(|error| panic!("admission: {error:?}"));
    let AlterPartitionReassignmentsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, submitted_plan, scratch, result_limit) = submission.into_parts();

    assert_eq!(
        host.reject_handoff(operation_id, plan("events"), scratch, result_limit),
        Err(AlterPartitionReassignmentsHostError::SubmissionMismatch)
    );
    host.reject_handoff(operation_id, submitted_plan, scratch, result_limit)
        .unwrap_or_else(|error| panic!("exact rejection evidence: {error}"));
    let AlterPartitionReassignmentsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("rejection observation: {error}"))
    else {
        panic!("driver rejection expected");
    };
    let (kind, delivery) = failure.into_parts();
    assert_eq!(kind, AlterPartitionReassignmentsFailureKind::DriverRejected);
    assert_eq!(delivery, AlterPartitionReassignmentsDeliveryStatus::NotSent);

    drop(host);
    stop_notifier(notifier);
}

#[test]
fn mismatched_accepted_call_remains_owned_and_blocks_recovery() {
    let (mut host, notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan("orders"))
        .unwrap_or_else(|error| panic!("admission: {error:?}"));
    let AlterPartitionReassignmentsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _plan, scratch, result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = AlterPartitionReassignmentsCall::submit(
        &driver,
        plan("events"),
        scratch,
        result_limit,
        submitted_deadline,
        Moment::from_tick(2),
    )
    .unwrap_or_else(|error| panic!("accepted mismatched call: {error}"));

    assert_eq!(
        host.accept_call(operation_id, call),
        Err(AlterPartitionReassignmentsHostError::SubmissionMismatch)
    );
    drop(driver);
    assert_eq!(
        host.recover_after_driver_shutdown(),
        Err(AlterPartitionReassignmentsHostError::SubmissionMismatch)
    );
    assert_eq!(host.unsettled(), 1);

    drop(admission);
    drop(host);
    stop_notifier(notifier);
}

fn plan(topic: &str) -> AlterPartitionReassignmentsPlan {
    AlterPartitionReassignmentsPlan::new(vec![AlterPartitionReassignment::new(
        topic.to_owned(),
        0,
        PartitionReassignmentTarget::Replicas(vec![1, 2]),
    )])
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn host() -> (AlterPartitionReassignmentsHost, AdminCompletionNotifier) {
    let (notifier, ports) = AdminCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start shared admin notifier: {error}"));
    (
        AlterPartitionReassignmentsHost::new(ports.alter_partition_reassignments),
        notifier,
    )
}

fn stop_notifier(mut notifier: AdminCompletionNotifier) {
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop admin notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join admin notifier: {error}"));
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
}
