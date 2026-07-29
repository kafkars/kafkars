//! Accepted-call completion and shutdown-recovery ownership scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{DescribeShareGroupMachineError, DescribeShareGroupPlan, Moment};

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::OperationDeadline,
    driver::{DescribeShareGroupCall, DriverOwner, RecoveredDescribeShareGroupCall},
};

use super::super::{DescribeShareGroupHost, DescribeShareGroupHostError, DescribeShareGroupTurn};
use crate::admin::describe_share_group::{
    DescribeShareGroupDeliveryStatus, DescribeShareGroupFailureKind, DescribeShareGroupOutcome,
};

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit API 77: {error:?}"));
    let DescribeShareGroupTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DescribeShareGroupHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit API 77: {error:?}"));
    host.operations[0].recovered_call = Some(RecoveredDescribeShareGroupCall::for_test());

    assert!(matches!(
        host.settle_recovered_transport(0),
        Err(DescribeShareGroupHostError::Machine(
            DescribeShareGroupMachineError::InvalidState
        ))
    ));
    assert!(host.operations[0].recovered_call.is_some());

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit API 77: {error:?}"));
    let DescribeShareGroupTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, _result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call =
        DescribeShareGroupCall::submit(&driver, &submitted_plan, submitted_deadline.transport())
            .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(DescribeShareGroupHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DescribeShareGroupOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DescribeShareGroupFailureKind::Transport,
            DescribeShareGroupDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn host() -> (DescribeShareGroupHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    (
        DescribeShareGroupHost::new(ports.describe_share_group),
        notifier,
    )
}

fn plan() -> DescribeShareGroupPlan {
    DescribeShareGroupPlan::new("payments-share".to_owned(), false)
        .unwrap_or_else(|error| panic!("valid API-77 plan: {error}"))
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(20),
        Instant::now() + Duration::from_secs(1),
    )
}

fn stop_notifier(notifier: &mut AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
