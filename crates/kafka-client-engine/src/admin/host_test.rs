//! Bounded `CreateTopics` admission, deadline, terminal, and byte-release scenarios.

use std::time::Instant;

use kafka_client_core::{
    CreateTopicSpecification, CreateTopicsInput, CreateTopicsPlan, DeliveryStatus,
};

use crate::clock::OperationDeadline;

use super::{
    CreateTopicsDeliveryStatus, CreateTopicsFailureKind, CreateTopicsHost, CreateTopicsOutcome,
    CreateTopicsTurn, host::CREATE_TOPICS_RETAINED_BYTES, test_support::create_topics_host,
    test_support::stop_notifier,
};

fn plan() -> CreateTopicsPlan {
    CreateTopicsPlan::new(
        vec![CreateTopicSpecification::new("orders", 3, -1, Vec::new())],
        false,
    )
    .unwrap_or_else(|error| panic!("valid CreateTopics plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}

#[test]
fn terminal_bytes_remain_reserved_until_observer_reclamation() {
    let (mut host, notifier) = create_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit CreateTopics: {error:?}"));
    let CreateTopicsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must be ready");
    };
    host.apply(submission.operation_id, CreateTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("apply driver acceptance: {error}"));
    host.apply(
        submission.operation_id,
        CreateTopicsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        },
    )
    .unwrap_or_else(|error| panic!("apply terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 16 * 1024);

    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe terminal: {error}"));
    let CreateTopicsOutcome::Failed(failure) = outcome else {
        panic!("transport failure expected");
    };
    assert_eq!(failure.kind(), CreateTopicsFailureKind::Transport);
    assert_eq!(failure.delivery(), CreateTopicsDeliveryStatus::PossiblySent);
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    stop(host, notifier);
}

#[test]
fn pre_driver_wait_expires_without_transport_ownership() {
    let (mut host, notifier) = create_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(2),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit CreateTopics: {error:?}"));
    assert!(matches!(
        host.turn(kafka_client_core::Moment::from_tick(2)),
        Ok(CreateTopicsTurn::Progress)
    ));
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe deadline terminal: {error}"));
    let CreateTopicsOutcome::Failed(failure) = outcome else {
        panic!("deadline failure expected");
    };
    assert_eq!(failure.kind(), CreateTopicsFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery(), CreateTopicsDeliveryStatus::NotSent);
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    stop(host, notifier);
}

#[test]
fn already_elapsed_start_publishes_its_terminal_without_submission() {
    let (mut host, notifier) = create_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(2),
            deadline(2),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit elapsed CreateTopics: {error:?}"));
    assert_eq!(host.unsettled(), 0);
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"));
    assert!(matches!(
        outcome,
        CreateTopicsOutcome::Failed(failure)
            if failure.kind() == CreateTopicsFailureKind::DeadlineElapsed
    ));
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim elapsed terminal: {error}"));
    stop(host, notifier);
}

#[test]
fn recovery_after_submission_handoff_is_conservatively_possibly_sent() {
    let (mut host, notifier) = create_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit CreateTopics: {error:?}"));
    let CreateTopicsTurn::Submit(_submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must cross the handoff boundary");
    };

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover handed-off submission: {error}"));
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery terminal: {error}"));
    let CreateTopicsOutcome::Failed(failure) = outcome else {
        panic!("transport failure expected");
    };
    assert_eq!(failure.kind(), CreateTopicsFailureKind::Transport);
    assert_eq!(failure.delivery(), CreateTopicsDeliveryStatus::PossiblySent);
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim recovery terminal: {error}"));
    stop(host, notifier);
}

#[test]
fn recovery_of_untouched_queued_submission_remains_not_sent() {
    let (mut host, notifier) = create_topics_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            16 * 1024,
        )
        .unwrap_or_else(|error| panic!("admit CreateTopics: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover queued submission: {error}"));
    let outcome = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery terminal: {error}"));
    let CreateTopicsOutcome::Failed(failure) = outcome else {
        panic!("driver rejection expected");
    };
    assert_eq!(failure.kind(), CreateTopicsFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), CreateTopicsDeliveryStatus::NotSent);
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim recovery terminal: {error}"));
    stop(host, notifier);
}

#[test]
fn retained_byte_limit_rejects_before_completion_reservation() {
    let (mut host, notifier) = create_topics_host();
    let rejection = host.try_admit(
        kafka_client_core::Moment::from_tick(1),
        deadline(10),
        plan(),
        CREATE_TOPICS_RETAINED_BYTES + 1,
    );
    assert!(matches!(
        rejection,
        Err(super::CreateTopicsAdmissionErrorKind::RetainedBytes)
    ));
    stop(host, notifier);
}

fn stop(host: CreateTopicsHost, notifier: super::completion::AdminCompletionNotifier) {
    drop(host);
    stop_notifier(notifier);
}
