//! Retained-envelope, deadline, recovery, and reclamation scenarios.

use std::sync::Arc;

use kafka_client_core::AdminListTransactionsPlan;

use crate::{
    admin::{AdminCompletionNotifier, AdminListTransactionsHost},
    clock::MonotonicClock,
};

use super::{
    AdminListTransactionsAdmissionErrorKind, AdminListTransactionsDeliveryStatus,
    AdminListTransactionsFailureKind, AdminListTransactionsOutcome,
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
    let (_operation_id, submitted_deadline, kind) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert!(matches!(
        kind,
        AdminListTransactionsSubmissionKind::Discovery
    ));

    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
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
