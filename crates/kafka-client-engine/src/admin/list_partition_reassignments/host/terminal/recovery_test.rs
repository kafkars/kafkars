//! Missing and mismatched accepted-call recovery scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    ListPartitionReassignmentTarget, ListPartitionReassignmentsInput,
    ListPartitionReassignmentsPlan, Moment,
};

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::OperationDeadline,
    driver::{DriverOwner, ListPartitionReassignmentsCall, ListPartitionReassignmentsRawTerminal},
};

use super::super::super::{
    ListPartitionReassignmentsDeliveryStatus, ListPartitionReassignmentsFailureKind,
    ListPartitionReassignmentsHost, ListPartitionReassignmentsHostError,
    ListPartitionReassignmentsOutcome, ListPartitionReassignmentsTurn,
};

#[test]
fn completion_fault_retains_exact_call_until_post_driver_recovery() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, ListPartitionReassignmentsPlan::all_active());
    let ListPartitionReassignmentsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, deadline, plan, result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = ListPartitionReassignmentsCall::submit(
        &driver,
        plan.clone(),
        result_limit,
        Moment::from_tick(2),
        deadline,
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(ListPartitionReassignmentsHostError::CallCompletion)
    ));
    assert!(host.call_matches_for_test(&plan, result_limit));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let ListPartitionReassignmentsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        failure.into_parts(),
        (
            ListPartitionReassignmentsFailureKind::Transport,
            ListPartitionReassignmentsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(notifier);
}

#[test]
fn handed_off_operation_without_a_call_cannot_forge_shutdown_settlement() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, ListPartitionReassignmentsPlan::all_active());
    let ListPartitionReassignmentsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(ListPartitionReassignmentsHostError::InvalidHandoff)
    ));
    assert!(host.publish_terminal_for_test().is_err());

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn mismatched_accepted_call_survives_recovery_and_blocks_publication() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, selected_plan(0));
    let ListPartitionReassignmentsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, deadline, _expected, result_limit) = submission.into_parts();
    let mismatch = selected_plan(1);
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = ListPartitionReassignmentsCall::submit(
        &driver,
        mismatch.clone(),
        result_limit,
        Moment::from_tick(2),
        deadline,
    )
    .unwrap_or_else(|error| panic!("accepted mismatched call: {error}"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(ListPartitionReassignmentsHostError::SubmissionMismatch)
    ));
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(ListPartitionReassignmentsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_matches_for_test(&mismatch, result_limit));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(ListPartitionReassignmentsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn mismatched_raw_terminal_cannot_settle_core_or_publish() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, selected_plan(0));
    let ListPartitionReassignmentsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, _expected, result_limit) = submission.into_parts();
    host.apply_input_for_test(
        operation_id,
        ListPartitionReassignmentsInput::DriverAccepted,
    )
    .unwrap_or_else(|error| panic!("accept synthetic driver ownership: {error}"));
    host.retain_raw_terminal_for_test(ListPartitionReassignmentsRawTerminal::for_test(
        selected_plan(1),
        result_limit,
    ));

    assert!(matches!(
        host.settle_raw_for_test(),
        Err(ListPartitionReassignmentsHostError::SubmissionMismatch)
    ));
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
) -> super::super::ListPartitionReassignmentsAdmission {
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
