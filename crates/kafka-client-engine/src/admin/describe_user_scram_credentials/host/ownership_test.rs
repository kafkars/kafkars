//! Exact rejection, call, terminal, and recovery ownership scenarios.

use std::sync::Arc;

use kafka_client_core::{
    DescribeUserScramCredentialsMachineError, DescribeUserScramCredentialsPlan,
};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, DescribeUserScramCredentialsHost},
    clock::MonotonicClock,
    driver::{DescribeUserScramCredentialsCall, DriverOwner},
};

use super::super::{
    DescribeUserScramCredentialsDeliveryStatus, DescribeUserScramCredentialsFailureKind,
    DescribeUserScramCredentialsHostError, DescribeUserScramCredentialsOutcome,
    DescribeUserScramCredentialsTurn,
};

#[test]
fn rejected_handoff_requires_exact_selection_order_and_all_user_semantics() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeUserScramCredentialsHost::new(ports.describe_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            selected_plan(&["zed", "alice"]),
        )
        .unwrap_or_else(|error| panic!("admit SCRAM query: {error:?}"));
    let DescribeUserScramCredentialsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, expected_plan, request_limit, result_limit) =
        submission.into_parts();

    assert!(matches!(
        host.reject_handoff(
            operation_id,
            selected_plan(&["alice", "zed"]),
            request_limit,
            result_limit,
        ),
        Err(DescribeUserScramCredentialsHostError::SubmissionMismatch)
    ));
    assert!(matches!(
        host.reject_handoff(operation_id, plan(None), request_limit, result_limit,),
        Err(DescribeUserScramCredentialsHostError::SubmissionMismatch)
    ));
    host.reject_handoff(operation_id, expected_plan, request_limit, result_limit)
        .unwrap_or_else(|error| panic!("reject exact handoff: {error}"));

    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_accepted_call_survives_driver_shutdown_as_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeUserScramCredentialsHost::new(ports.describe_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(None))
        .unwrap_or_else(|error| panic!("admit SCRAM query: {error:?}"));
    let DescribeUserScramCredentialsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _plan, request_limit, result_limit) =
        submission.into_parts();
    let mismatched_plan = selected_plan(&["alice"]);
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeUserScramCredentialsCall::submit(
        &driver,
        mismatched_plan.clone(),
        request_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(DescribeUserScramCredentialsHostError::SubmissionMismatch)
    ));
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DescribeUserScramCredentialsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_matches_for_test(&mismatched_plan, request_limit, result_limit));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DescribeUserScramCredentialsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_selection_survive_core_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeUserScramCredentialsHost::new(ports.describe_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(Some("alice")),
        )
        .unwrap_or_else(|error| panic!("admit SCRAM query: {error:?}"));
    let (request_limit, result_limit) = host.bounds_for_test();
    host.retain_recovered_call_for_test(plan(Some("alice")), request_limit, result_limit);

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(DescribeUserScramCredentialsHostError::Machine(
            DescribeUserScramCredentialsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DescribeUserScramCredentialsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_raw_terminal_blocks_settlement_and_publication() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeUserScramCredentialsHost::new(ports.describe_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            selected_plan(&["zed", "alice"]),
        )
        .unwrap_or_else(|error| panic!("admit SCRAM query: {error:?}"));
    let DescribeUserScramCredentialsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, request_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeUserScramCredentialsCall::submit(
        &driver,
        submitted_plan,
        request_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);
    host.replace_call_with_raw_for_test(
        selected_plan(&["alice", "zed"]),
        request_limit,
        result_limit,
    );

    assert!(matches!(
        host.settle_raw_for_test(),
        Err(DescribeUserScramCredentialsHostError::SubmissionMismatch)
    ));
    assert!(host.raw_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DescribeUserScramCredentialsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeUserScramCredentialsHost::new(ports.describe_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(Some("alice")),
        )
        .unwrap_or_else(|error| panic!("admit SCRAM query: {error:?}"));
    let DescribeUserScramCredentialsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, request_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeUserScramCredentialsCall::submit(
        &driver,
        submitted_plan,
        request_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now()),
        Err(DescribeUserScramCredentialsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DescribeUserScramCredentialsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        failure.kind(),
        &DescribeUserScramCredentialsFailureKind::Transport
    );
    assert_eq!(
        failure.delivery(),
        DescribeUserScramCredentialsDeliveryStatus::PossiblySent
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn plan(user: Option<&str>) -> DescribeUserScramCredentialsPlan {
    DescribeUserScramCredentialsPlan::new(user.map(|user| vec![user.to_owned()]))
        .unwrap_or_else(|error| panic!("valid user selection: {error}"))
}

fn selected_plan(users: &[&str]) -> DescribeUserScramCredentialsPlan {
    DescribeUserScramCredentialsPlan::new(Some(
        users.iter().map(|user| (*user).to_owned()).collect(),
    ))
    .unwrap_or_else(|error| panic!("valid user selection: {error}"))
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
