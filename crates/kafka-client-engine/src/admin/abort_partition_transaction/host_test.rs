//! Bounded ownership tests for one partition transaction-abort host.

use std::sync::Arc;

use kafka_client_core::{AbortPartitionTransactionPlan, Moment};

use crate::{
    admin::{
        ABORT_PARTITION_TRANSACTION_CAPACITY, AbortPartitionTransactionAdmissionErrorKind,
        AbortPartitionTransactionDeliveryStatus, AbortPartitionTransactionFailureKind,
        AbortPartitionTransactionOutcome, AbortPartitionTransactionTurn, AdminCompletionNotifier,
        abort_partition_transaction::host::ABORT_PARTITION_TRANSACTION_RETAINED_BYTES,
    },
    clock::{MonotonicClock, OperationDeadline},
};

use super::AbortPartitionTransactionHost;

fn deadline() -> OperationDeadline {
    Arc::new(MonotonicClock::new())
        .capture_deadline_after(std::time::Duration::from_secs(1))
        .expect("deadline")
        .operation_deadline()
}

fn plan(topic: &str) -> AbortPartitionTransactionPlan {
    AbortPartitionTransactionPlan::new(topic.to_owned(), 3, 41, 7, 11).expect("valid plan")
}

#[test]
fn admission_reserves_capacity_and_retained_bytes() {
    let (mut notifier, ports) = AdminCompletionNotifier::start().expect("completion notifier");
    let mut host = AbortPartitionTransactionHost::new(ports.abort_partition_transaction);

    let admission = host
        .try_admit(Moment::from_tick(0), deadline(), plan("orders"))
        .expect("admit");

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert!(host.retained_bytes_for_test() > 0);
    assert!(host.retained_bytes_for_test() < ABORT_PARTITION_TRANSACTION_RETAINED_BYTES);
    drop(admission.observer);
    host.recover_after_driver_shutdown().expect("recover host");
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn elapsed_at_start_publishes_not_sent_deadline_terminal() {
    let (mut notifier, ports) = AdminCompletionNotifier::start().expect("completion notifier");
    let mut host = AbortPartitionTransactionHost::new(ports.abort_partition_transaction);
    let deadline = deadline();
    let now = Moment::from_tick(deadline.core().tick());

    let admission = host
        .try_admit(now, deadline, plan("orders"))
        .expect("accepted terminal");
    let outcome = admission.observer.wait().expect("observe");
    let AbortPartitionTransactionOutcome::Failed(failure) = outcome else {
        panic!("expected deadline failure");
    };
    assert_eq!(
        failure.kind(),
        super::AbortPartitionTransactionFailureKind::DeadlineElapsed
    );
    assert_eq!(
        failure.delivery(),
        super::AbortPartitionTransactionDeliveryStatus::NotSent
    );
    let _turn = host.turn(now).expect("reclaim terminal");
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn operation_count_is_bounded_at_sixteen() {
    let (mut notifier, ports) = AdminCompletionNotifier::start().expect("completion notifier");
    let mut host = AbortPartitionTransactionHost::new(ports.abort_partition_transaction);
    let deadline = deadline();
    let mut observers = Vec::new();

    for producer_id in 0..ABORT_PARTITION_TRANSACTION_CAPACITY {
        let plan = AbortPartitionTransactionPlan::new(
            "orders".to_owned(),
            3,
            i64::try_from(producer_id).expect("bounded producer"),
            7,
            11,
        )
        .expect("valid plan");
        observers.push(
            host.try_admit(Moment::from_tick(0), deadline, plan)
                .expect("bounded admission")
                .observer,
        );
    }
    assert!(matches!(
        host.try_admit(Moment::from_tick(0), deadline, plan("overflow")),
        Err(AbortPartitionTransactionAdmissionErrorKind::Capacity)
    ));

    drop(observers);
    host.recover_after_driver_shutdown().expect("recover host");
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn recovery_distinguishes_untouched_from_handed_off_delivery() {
    for handed_off in [false, true] {
        let (mut notifier, ports) = AdminCompletionNotifier::start().expect("completion notifier");
        let mut host = AbortPartitionTransactionHost::new(ports.abort_partition_transaction);
        let deadline = deadline();
        let admission = host
            .try_admit(Moment::from_tick(0), deadline, plan("orders"))
            .expect("admit");
        if handed_off {
            let AbortPartitionTransactionTurn::Submit(_) =
                host.turn(Moment::from_tick(0)).expect("take submission")
            else {
                panic!("submission expected");
            };
        }

        host.recover_after_driver_shutdown().expect("recover host");
        let AbortPartitionTransactionOutcome::Failed(failure) =
            admission.observer.wait().expect("observe")
        else {
            panic!("failure expected");
        };
        let expected = if handed_off {
            (
                AbortPartitionTransactionFailureKind::Transport,
                AbortPartitionTransactionDeliveryStatus::PossiblySent,
            )
        } else {
            (
                AbortPartitionTransactionFailureKind::DriverRejected,
                AbortPartitionTransactionDeliveryStatus::NotSent,
            )
        };
        assert_eq!((failure.kind(), failure.delivery()), expected);

        let _turn = host.turn(Moment::from_tick(0)).expect("reclaim terminal");
        drop(host);
        stop_notifier(&mut notifier);
    }
}

fn stop_notifier(notifier: &mut AdminCompletionNotifier) {
    notifier
        .stop()
        .expect("stop notifier")
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
