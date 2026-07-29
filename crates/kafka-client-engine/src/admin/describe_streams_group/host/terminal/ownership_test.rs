//! Accepted-call completion and post-driver recovery ownership scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{DescribeStreamsGroupMachineError, DescribeStreamsGroupPlan, Moment};

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::OperationDeadline,
    driver::{DescribeStreamsGroupCall, DriverOwner, RecoveredDescribeStreamsGroupCall},
};

use super::super::{
    DescribeStreamsGroupHandoff, DescribeStreamsGroupHost, DescribeStreamsGroupHostError,
    DescribeStreamsGroupTurn,
};

#[test]
fn completion_fault_retains_call_for_post_driver_recovery() {
    let (mut host, notifier) = host();
    let deadline = deadline(10);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admission: {error:?}"));
    let DescribeStreamsGroupTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, _result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call =
        DescribeStreamsGroupCall::submit(&driver, &submitted_plan, submitted_deadline.transport())
            .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(DescribeStreamsGroupHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"));
    let super::super::super::DescribeStreamsGroupOutcome::Failed(failure) = outcome else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery(),),
        (
            super::super::super::DescribeStreamsGroupFailureKind::Transport,
            super::super::super::DescribeStreamsGroupDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admission: {error:?}"));
    host.operations[0].recovered_call = Some(RecoveredDescribeStreamsGroupCall::for_test());

    assert!(matches!(
        host.settle_recovered_transport(0),
        Err(DescribeStreamsGroupHostError::Machine(
            DescribeStreamsGroupMachineError::InvalidState
        ))
    ));
    assert!(host.operations[0].recovered_call.is_some());

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn handed_off_recovery_without_the_exact_call_is_invalid() {
    let (mut host, notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admission: {error:?}"));
    let DescribeStreamsGroupTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    assert_eq!(
        host.operations[0].handoff,
        DescribeStreamsGroupHandoff::HandedOff
    );

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DescribeStreamsGroupHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

fn host() -> (DescribeStreamsGroupHost, AdminCompletionNotifier) {
    let (notifier, ports) = AdminCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start shared admin notifier: {error}"));
    (
        DescribeStreamsGroupHost::new(ports.describe_streams_group),
        notifier,
    )
}

fn plan() -> DescribeStreamsGroupPlan {
    DescribeStreamsGroupPlan::new("streams-app".to_owned(), false, false)
        .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
}

fn stop_notifier(mut notifier: AdminCompletionNotifier) {
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop shared admin notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join shared admin notifier: {error}"));
}
