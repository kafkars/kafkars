//! Completion-fault, correlation-mismatch, and raw-terminal ownership scenarios.

use std::sync::Arc;

use kafka_client_core::{AdminListTransactionsInput, AdminListTransactionsPlan};

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::MonotonicClock,
    driver::{DriverOwner, ListTransactionsCall, ListTransactionsRawTerminal},
};

use super::super::super::{
    AdminListTransactionsDeliveryStatus, AdminListTransactionsFailureKind,
    AdminListTransactionsHost, AdminListTransactionsHostError, AdminListTransactionsOutcome,
    AdminListTransactionsSubmissionKind, AdminListTransactionsTurn,
};

#[test]
fn completion_fault_retains_discovery_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminListTransactionsHost::new(ports.list_transactions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit ListTransactions: {error:?}"));
    let (operation_id, deadline, retained_limit) = discovery_submission(&mut host, &capture);
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call =
        ListTransactionsCall::submit_discovery(&driver, retained_limit, deadline.transport())
            .unwrap_or_else(|_error| panic!("accepted discovery"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now()),
        Err(AdminListTransactionsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AdminListTransactionsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AdminListTransactionsFailureKind::Transport,
            AdminListTransactionsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_accepted_discovery_survives_as_recovered_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminListTransactionsHost::new(ports.list_transactions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit ListTransactions: {error:?}"));
    let (operation_id, deadline, retained_limit) = discovery_submission(&mut host, &capture);
    let mismatched_limit = retained_limit + 1;
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call =
        ListTransactionsCall::submit_discovery(&driver, mismatched_limit, deadline.transport())
            .unwrap_or_else(|_error| panic!("accepted discovery"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(AdminListTransactionsHostError::SubmissionMismatch)
    ));
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AdminListTransactionsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_matches_discovery_for_test(mismatched_limit));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AdminListTransactionsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_raw_discovery_is_rejected_before_core_settlement() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminListTransactionsHost::new(ports.list_transactions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit ListTransactions: {error:?}"));
    let (operation_id, _deadline, retained_limit) = discovery_submission(&mut host, &capture);
    host.apply_input_for_test(operation_id, AdminListTransactionsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept synthetic driver ownership: {error}"));
    host.retain_raw_terminal_for_test(ListTransactionsRawTerminal::discovery_for_test(
        retained_limit + 1,
    ));

    assert!(matches!(
        host.settle_raw_for_test(),
        Err(AdminListTransactionsHostError::SubmissionMismatch)
    ));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AdminListTransactionsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

fn discovery_submission(
    host: &mut AdminListTransactionsHost,
    capture: &crate::clock::DeadlineCapture,
) -> (
    kafka_client_core::OperationId,
    crate::clock::OperationDeadline,
    usize,
) {
    let AdminListTransactionsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("discovery submission expected");
    };
    let (operation_id, deadline, kind) = submission.into_parts();
    let AdminListTransactionsSubmissionKind::Discovery { retained_limit } = kind else {
        panic!("discovery submission expected");
    };
    (operation_id, deadline, retained_limit)
}

fn plan() -> AdminListTransactionsPlan {
    AdminListTransactionsPlan::new(
        vec!["Ongoing".to_owned()],
        vec![-7],
        Some(42),
        Some("^orders".to_owned()),
    )
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
