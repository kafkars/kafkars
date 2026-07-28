//! Stale automatic-partition lookup settlement after terminal waiting transitions.

#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test assertions fail immediately on invalid ownership outcomes"
)]

use kafka_client_core::{Moment, ProducerCancellationOutcome};

use crate::ProducerDeliveryFailureKind;

use super::{
    super::host_limits_test::start,
    partitioning_test::{admit_automatic, assert_not_sent, topic_view},
};

#[test]
fn deadline_and_cancellation_release_waiter_while_lookup_drains_stale() {
    let mut timed = start(super::partitioning_test::partitioning_limits());
    let accepted = admit_automatic(&mut timed, None, 2);
    let request = timed
        .take_partitioning_request()
        .unwrap()
        .expect("deadline metadata request");
    let (_id, observer, _token) = accepted.into_parts();
    timed.drive_waiting(Moment::from_tick(2), 1).unwrap();
    assert_not_sent(
        observer.wait(),
        ProducerDeliveryFailureKind::DeadlineElapsed,
    );
    assert!(
        !timed
            .apply_partitioning_view(request, &topic_view())
            .unwrap()
    );

    let mut cancelled = start(super::partitioning_test::partitioning_limits());
    let accepted = admit_automatic(&mut cancelled, None, 500);
    let request = cancelled
        .take_partitioning_request()
        .unwrap()
        .expect("cancel metadata request");
    let (id, observer, token) = accepted.into_parts();
    assert_eq!(
        cancelled.try_cancel_waiter(id, &token).unwrap(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    assert_not_sent(observer.wait(), ProducerDeliveryFailureKind::Cancelled);
    assert!(
        !cancelled
            .apply_partitioning_view(request, &topic_view())
            .unwrap()
    );
}

#[test]
fn close_and_shutdown_settle_waiter_while_lookup_drains_stale() {
    for shutdown in [false, true] {
        let mut host = start(super::partitioning_test::partitioning_limits());
        let accepted = admit_automatic(&mut host, None, 500);
        let request = host
            .take_partitioning_request()
            .unwrap()
            .expect("fenced metadata request");
        let (_id, observer, _token) = accepted.into_parts();
        if shutdown {
            host.execution_unavailable(Moment::from_tick(1)).unwrap();
        } else {
            host.close_admission();
            host.drive_waiting(Moment::from_tick(1), 1).unwrap();
        }
        assert_not_sent(
            observer.wait(),
            ProducerDeliveryFailureKind::ExecutionUnavailable,
        );
        assert!(
            !host
                .apply_partitioning_view(request, &topic_view())
                .unwrap()
        );
    }
}
