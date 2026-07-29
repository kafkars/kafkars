//! Retained-envelope, deadline, recovery, and reclamation scenarios.

use std::sync::Arc;

use kafka_client_core::AdminDescribeTransactionsPlan;

use crate::{
    admin::{AdminCompletionNotifier, AdminDescribeTransactionsHost},
    clock::MonotonicClock,
};

use super::{
    AdminDescribeTransactionsAdmissionErrorKind, AdminDescribeTransactionsDeliveryStatus,
    AdminDescribeTransactionsFailureKind, AdminDescribeTransactionsOutcome,
    AdminDescribeTransactionsTurn, host::ADMIN_DESCRIBE_TRANSACTIONS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_full_envelope_before_first_id() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AdminDescribeTransactionsHost::new(ports.describe_transactions);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit DescribeTransactions: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        ADMIN_DESCRIBE_TRANSACTIONS_RETAINED_BYTES
    );
    assert_eq!(
        host.next_deadline(),
        Some(capture.operation_deadline().core())
    );
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan()),
        Err(AdminDescribeTransactionsAdmissionErrorKind::RetainedBytes)
    ));

    let AdminDescribeTransactionsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, transactional_id) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(transactional_id, "orders-writer");
    assert_eq!(host.next_deadline(), None);

    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_and_handed_off_recovery_preserve_delivery_certainty() {
    for handed_off in [false, true] {
        let (mut notifier, ports) =
            AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
        let mut host = AdminDescribeTransactionsHost::new(ports.describe_transactions);
        let capture = deadline();
        let admission = host
            .try_admit(capture.now(), capture.operation_deadline(), plan())
            .unwrap_or_else(|error| panic!("admit DescribeTransactions: {error:?}"));
        if handed_off {
            let AdminDescribeTransactionsTurn::Submit(_) = host
                .turn(capture.now())
                .unwrap_or_else(|error| panic!("take submission: {error}"))
            else {
                panic!("submission expected");
            };
        }

        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover host: {error}"));
        let AdminDescribeTransactionsOutcome::Failed(failure) = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe recovery: {error}"))
        else {
            panic!("failure expected");
        };
        let expected = if handed_off {
            (
                AdminDescribeTransactionsFailureKind::Transport,
                AdminDescribeTransactionsDeliveryStatus::PossiblySent,
            )
        } else {
            (
                AdminDescribeTransactionsFailureKind::DriverRejected,
                AdminDescribeTransactionsDeliveryStatus::NotSent,
            )
        };
        assert_eq!((failure.kind(), failure.delivery()), expected);
        let _progress = host
            .turn(capture.now())
            .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
        assert_eq!(host.retained_bytes_for_test(), 0);
        drop(host);
        stop_notifier(&mut notifier);
    }
}

fn plan() -> AdminDescribeTransactionsPlan {
    AdminDescribeTransactionsPlan::new(vec!["orders-writer".to_owned(), "audit-writer".to_owned()])
        .unwrap_or_else(|error| panic!("valid DescribeTransactions plan: {error}"))
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
