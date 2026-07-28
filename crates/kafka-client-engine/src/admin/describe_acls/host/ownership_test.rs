//! Exact rejection and accepted-call correlation scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{Deadline, DescribeAclsFilter, DescribeAclsPlan, Moment};

use crate::{
    EngineConfig,
    admin::{
        AdminCompletionNotifier, DescribeAclsDeliveryStatus, DescribeAclsFailureKind,
        DescribeAclsOutcome,
    },
    clock::OperationDeadline,
    driver::{DescribeAclsCall, DriverOwner},
};

use super::{DescribeAclsHost, DescribeAclsHostError, DescribeAclsTurn};

#[test]
fn mismatched_rejection_cannot_settle_the_query() {
    let (mut host, notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit query: {error:?}"));
    let DescribeAclsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, submitted_plan, result_limit) = submission.into_parts();

    assert_eq!(
        host.reject_handoff(operation_id, plan("payments"), result_limit),
        Err(DescribeAclsHostError::SubmissionMismatch)
    );
    host.reject_handoff(operation_id, submitted_plan, result_limit)
        .unwrap_or_else(|error| panic!("reject exact query: {error}"));
    let DescribeAclsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe rejection: {error}"))
    else {
        panic!("driver rejection expected");
    };
    assert_eq!(failure.kind(), &DescribeAclsFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), DescribeAclsDeliveryStatus::NotSent);

    drop(host);
    stop_notifier(notifier);
}

#[test]
fn mismatched_accepted_call_remains_owned_and_blocks_recovery() {
    let (mut host, notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit query: {error:?}"));
    let DescribeAclsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _submitted_plan, result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeAclsCall::submit(
        &driver,
        plan("payments"),
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted mismatched call"));

    assert_eq!(
        host.accept_call(operation_id, call),
        Err(DescribeAclsHostError::SubmissionMismatch)
    );
    drop(driver);
    assert_eq!(
        host.recover_after_driver_shutdown(),
        Err(DescribeAclsHostError::SubmissionMismatch)
    );
    assert_eq!(host.unsettled(), 1);

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn mismatched_raw_terminal_cannot_reach_core_or_publication() {
    let (mut host, notifier) = host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan("orders"))
        .unwrap_or_else(|error| panic!("admit query: {error:?}"));
    let DescribeAclsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, _deadline, _submitted_plan, result_limit) = submission.into_parts();
    host.retain_raw_terminal_for_test(plan("payments"), result_limit);

    assert_eq!(
        host.recover_after_driver_shutdown(),
        Err(DescribeAclsHostError::SubmissionMismatch)
    );
    assert_eq!(host.unsettled(), 1);

    drop((admission, host));
    stop_notifier(notifier);
}

fn plan(resource_name: &str) -> DescribeAclsPlan {
    DescribeAclsPlan::new(DescribeAclsFilter::new(
        2,
        Some(resource_name.to_owned()),
        3,
        None,
        None,
        1,
        1,
    ))
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn host() -> (DescribeAclsHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    (DescribeAclsHost::new(ports.describe_acls), notifier)
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
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
