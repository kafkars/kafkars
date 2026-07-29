//! Retained-envelope, deadline, recovery, and reclamation scenarios.

use std::sync::Arc;

use kafka_client_core::AdminListTransactionsPlan;

use crate::{
    admin::{AdminCompletionNotifier, AdminListTransactionsHost},
    clock::MonotonicClock,
};

use super::{
    AdminListTransactionsAdmissionErrorKind, AdminListTransactionsDeliveryStatus,
    AdminListTransactionsFailureKind, AdminListTransactionsHostError, AdminListTransactionsOutcome,
    AdminListTransactionsSubmissionKind, AdminListTransactionsTurn,
    host::ADMIN_LIST_TRANSACTIONS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_full_envelope_before_discovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminListTransactionsHost::new(ports.list_transactions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit ListTransactions: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        ADMIN_LIST_TRANSACTIONS_RETAINED_BYTES
    );
    assert_eq!(
        host.next_deadline(),
        Some(capture.operation_deadline().core())
    );
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan()),
        Err(AdminListTransactionsAdmissionErrorKind::RetainedBytes)
    ));

    let AdminListTransactionsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("discovery submission expected");
    };
    let (operation_id, submitted_deadline, kind) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    let AdminListTransactionsSubmissionKind::Discovery { retained_limit } = kind else {
        panic!("discovery submission expected");
    };
    assert!(retained_limit < ADMIN_LIST_TRANSACTIONS_RETAINED_BYTES);

    host.reject_handoff(
        operation_id,
        AdminListTransactionsSubmissionKind::Discovery { retained_limit },
    )
    .unwrap_or_else(|error| panic!("return exact rejected discovery: {error}"));
    drop(admission.observer);
    stop_notifier(&mut notifier);
    let _ = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("reclaim rejected discovery: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
}

#[test]
fn rejected_handoff_requires_the_exact_discovery_limit() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminListTransactionsHost::new(ports.list_transactions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit ListTransactions: {error:?}"));
    let AdminListTransactionsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("discovery submission expected");
    };
    let (operation_id, _deadline, submission) = submission.into_parts();
    let AdminListTransactionsSubmissionKind::Discovery { retained_limit } = submission else {
        panic!("discovery submission expected");
    };

    assert!(matches!(
        host.reject_handoff(
            operation_id,
            AdminListTransactionsSubmissionKind::Discovery {
                retained_limit: retained_limit + 1,
            },
        ),
        Err(AdminListTransactionsHostError::SubmissionMismatch)
    ));
    host.reject_handoff(
        operation_id,
        AdminListTransactionsSubmissionKind::Discovery { retained_limit },
    )
    .unwrap_or_else(|error| panic!("reject exact discovery: {error}"));
    let AdminListTransactionsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe rejection: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AdminListTransactionsFailureKind::DriverRejected,
            AdminListTransactionsDeliveryStatus::NotSent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_recovery_is_definitely_unsent() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminListTransactionsHost::new(ports.list_transactions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit ListTransactions: {error:?}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    let AdminListTransactionsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AdminListTransactionsFailureKind::DriverRejected,
            AdminListTransactionsDeliveryStatus::NotSent,
        )
    );
    let _ = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("reclaim: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    stop_notifier(&mut notifier);
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
