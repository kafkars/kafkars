//! Bounded `DeleteTopics` deadline, recovery, and byte-release scenarios.

use std::time::Instant;

use kafka_client_core::{DeleteTopicsInput, DeleteTopicsPlan, DeliveryStatus};

use crate::clock::OperationDeadline;

use super::{
    DeleteTopicsDeliveryStatus, DeleteTopicsFailureKind, DeleteTopicsHost, DeleteTopicsOutcome,
    DeleteTopicsTurn,
};

fn plan() -> DeleteTopicsPlan {
    DeleteTopicsPlan::new(vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("valid deletion plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}

#[test]
fn terminal_bytes_remain_reserved_until_observer_reclamation() {
    let mut host =
        DeleteTopicsHost::new().unwrap_or_else(|error| panic!("start deletion host: {error}"));
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit deletion: {error:?}"));
    let DeleteTopicsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must be ready");
    };
    host.apply(submission.operation_id, DeleteTopicsInput::DriverAccepted)
        .and_then(|()| {
            host.apply(
                submission.operation_id,
                DeleteTopicsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                },
            )
        })
        .unwrap_or_else(|error| panic!("settle deletion: {error}"));
    assert_eq!(host.retained_bytes, 16 * 1024);
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe terminal: {error}"));
    assert!(matches!(
        outcome,
        DeleteTopicsOutcome::Failed(failure)
            if failure.kind() == DeleteTopicsFailureKind::Transport
                && failure.delivery() == DeleteTopicsDeliveryStatus::PossiblySent
    ));
    let _turn = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    assert_eq!(host.retained_bytes, 0);
    stop(host);
}

#[test]
fn queued_and_taken_recovery_preserve_the_last_not_sent_boundary() {
    let (mut queued, queued_admission) = admitted();
    queued
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover queued deletion: {error}"));
    assert_failure(
        queued_admission.observer.wait(),
        DeleteTopicsFailureKind::DriverRejected,
        DeleteTopicsDeliveryStatus::NotSent,
    );
    let _turn = queued
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim queued result: {error}"));
    stop(queued);

    let (mut taken, taken_admission) = admitted();
    assert!(matches!(
        taken.turn(kafka_client_core::Moment::from_tick(2)),
        Ok(DeleteTopicsTurn::Submit(_))
    ));
    taken
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover taken deletion: {error}"));
    assert_failure(
        taken_admission.observer.wait(),
        DeleteTopicsFailureKind::Transport,
        DeleteTopicsDeliveryStatus::PossiblySent,
    );
    let _turn = taken
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim taken result: {error}"));
    stop(taken);
}

fn admitted() -> (DeleteTopicsHost, super::delete_host::DeleteTopicsAdmission) {
    let mut host =
        DeleteTopicsHost::new().unwrap_or_else(|error| panic!("start deletion host: {error}"));
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit deletion: {error:?}"));
    (host, admission)
}

fn assert_failure(
    outcome: Result<DeleteTopicsOutcome, super::DeleteTopicsObserverError>,
    kind: DeleteTopicsFailureKind,
    delivery: DeleteTopicsDeliveryStatus,
) {
    let DeleteTopicsOutcome::Failed(failure) =
        outcome.unwrap_or_else(|error| panic!("observe recovery terminal: {error}"))
    else {
        panic!("whole-operation failure expected");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}

fn stop(mut host: DeleteTopicsHost) {
    let notifier = host
        .stop_notifier()
        .unwrap_or_else(|error| panic!("stop deletion notifier: {error}"));
    notifier
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("join deletion notifier: {error}"));
}
