//! Exact ordered route, rejection, accepted-call, raw-terminal, and recovery evidence.

use std::sync::Arc;

use kafka_client_core::{AdminDescribeConsumerGroupsCallKind, AdminDescribeConsumerGroupsPlan};

use crate::{
    EngineConfig,
    admin::{
        AdminCompletionNotifier, DescribeConsumerGroupsHost, DescribeConsumerGroupsHostError,
        DescribeConsumerGroupsTurn,
    },
    clock::MonotonicClock,
    driver::{DescribeConsumerGroupsCall, DriverOwner},
};

#[test]
fn rejection_requires_exact_first_route_intent_and_both_bounds() {
    let (mut host, mut notifier, clock) = host();
    let capture = deadline(&clock);
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    assert_eq!(host.route_plan_for_test(), ["zeta", "alpha", "omega"]);
    let DescribeConsumerGroupsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _, group_id, authorized, call_kind, request_limit, result_limit) =
        submission.into_parts();

    assert!(matches!(
        host.reject_handoff(
            operation_id,
            "alpha".to_owned(),
            authorized,
            call_kind,
            request_limit,
            result_limit,
        ),
        Err(DescribeConsumerGroupsHostError::SubmissionMismatch)
    ));
    assert!(matches!(
        host.reject_handoff(
            operation_id,
            group_id.clone(),
            !authorized,
            call_kind,
            request_limit,
            result_limit,
        ),
        Err(DescribeConsumerGroupsHostError::SubmissionMismatch)
    ));
    assert!(matches!(
        host.reject_handoff(
            operation_id,
            group_id.clone(),
            authorized,
            call_kind,
            request_limit,
            result_limit - 1,
        ),
        Err(DescribeConsumerGroupsHostError::SubmissionMismatch)
    ));
    host.reject_handoff(
        operation_id,
        group_id,
        authorized,
        call_kind,
        request_limit,
        result_limit,
    )
    .unwrap_or_else(|error| panic!("reject exact evidence: {error}"));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_accepted_route_survives_shutdown_and_blocks_publication() {
    let (mut host, mut notifier, clock) = host();
    let capture = deadline(&clock);
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let DescribeConsumerGroupsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, deadline, _, authorized, call_kind, request_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeConsumerGroupsCall::submit(
        &driver,
        call_kind,
        "alpha".to_owned(),
        authorized,
        request_limit,
        result_limit,
        deadline,
    )
    .unwrap_or_else(|_error| panic!("accepted call"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(DescribeConsumerGroupsHostError::SubmissionMismatch)
    ));
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DescribeConsumerGroupsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_matches_for_test(
        "alpha",
        authorized,
        call_kind,
        request_limit,
        result_limit,
    ));
    assert_eq!(host.route_plan_for_test(), ["zeta", "alpha", "omega"]);
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DescribeConsumerGroupsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_raw_capacity_blocks_core_settlement_and_publication() {
    let (mut host, mut notifier, clock) = host();
    let capture = deadline(&clock);
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let DescribeConsumerGroupsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, deadline, group_id, authorized, call_kind, request_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeConsumerGroupsCall::submit(
        &driver,
        call_kind,
        group_id.clone(),
        authorized,
        request_limit,
        result_limit,
        deadline,
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    drop(driver);
    host.replace_call_with_raw_for_test(
        group_id,
        authorized,
        call_kind,
        request_limit,
        result_limit - 1,
    );

    assert!(matches!(
        host.settle_raw_for_test(),
        Err(DescribeConsumerGroupsHostError::SubmissionMismatch)
    ));
    assert!(host.raw_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DescribeConsumerGroupsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

fn host() -> (
    DescribeConsumerGroupsHost,
    AdminCompletionNotifier,
    Arc<MonotonicClock>,
) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    (
        DescribeConsumerGroupsHost::new(ports.describe_consumer_groups),
        notifier,
        Arc::new(MonotonicClock::new()),
    )
}

fn deadline(clock: &Arc<MonotonicClock>) -> crate::clock::DeadlineCapture {
    clock
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

fn plan() -> AdminDescribeConsumerGroupsPlan {
    AdminDescribeConsumerGroupsPlan::new(
        ["zeta", "alpha", "omega"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        true,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

#[test]
fn call_kind_is_part_of_rejection_identity() {
    let (mut host, mut notifier, clock) = host();
    let capture = deadline(&clock);
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let DescribeConsumerGroupsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _, group_id, authorized, call_kind, request_limit, result_limit) =
        submission.into_parts();
    assert!(matches!(
        host.reject_handoff(
            operation_id,
            group_id.clone(),
            authorized,
            AdminDescribeConsumerGroupsCallKind::Classic,
            request_limit,
            result_limit,
        ),
        Err(DescribeConsumerGroupsHostError::SubmissionMismatch)
    ));
    host.reject_handoff(
        operation_id,
        group_id,
        authorized,
        call_kind,
        request_limit,
        result_limit,
    )
    .unwrap_or_else(|error| panic!("reject exact call kind: {error}"));
    drop((admission, host));
    stop_notifier(&mut notifier);
}
