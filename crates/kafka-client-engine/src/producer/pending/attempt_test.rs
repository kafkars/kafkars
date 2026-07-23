//! Coordinated attempt misuse, record transfer, and drop ownership scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{
    Deadline, DeliveryStatus, PartitionIndex, ProducerCompletion, ProducerFailure,
};

use super::{
    PendingAdmissionRegistry, PendingAttemptRestoreError, PendingAttemptStateError,
    PendingCellError, PendingRecordTransferState, ProducerSendFailure, ProducerSendFailureKind,
};
use crate::{
    ProducerDeliveryObserver,
    clock::OperationDeadline,
    completion::{CompletionRegistry, ReclaimStatus},
    producer::ProducerRecord,
};

#[test]
fn accept_before_record_commit_returns_observer_attempt_and_exact_record() {
    let mut pending = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = register(&mut pending, "orders");
    let send = registration.into_send();
    let attempt = take(&mut pending);
    let (mut completions, completion_id, observer) = observer();

    let failure = attempt
        .accept(observer)
        .err()
        .unwrap_or_else(|| panic!("unadmitted record must reject accepted resolution"));
    let (error, attempt, observer) = failure.into_parts();
    assert_eq!(error, PendingAttemptStateError::RecordNotCommitted);
    complete_observer(&mut completions, completion_id, observer);
    let local = attempt
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|_failure| panic!("unchanged attempt should settle locally"));
    let (admission, job) = local.into_parts();
    assert_eq!(admission.into_record().topic().as_ref(), "orders");
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}

#[test]
fn detached_record_cannot_resolve_and_returns_intact_to_same_attempt() {
    let mut pending = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = register(&mut pending, "detached");
    let send = registration.into_send();
    let mut attempt = take(&mut pending);
    let record = attempt
        .detach_record()
        .unwrap_or_else(|error| panic!("record should detach: {error:?}"));
    let expected_topic = Arc::clone(record.topic());
    let (mut completions, completion_id, observer) = observer();

    let accept = attempt
        .accept(observer)
        .err()
        .unwrap_or_else(|| panic!("detached record must not accept"));
    let (error, attempt, observer) = accept.into_parts();
    assert_eq!(error, PendingAttemptStateError::RecordNotCommitted);
    complete_observer(&mut completions, completion_id, observer);
    let requested = ProducerSendFailure::new(ProducerSendFailureKind::Closed);
    let settle = attempt
        .settle_local(requested)
        .err()
        .unwrap_or_else(|| panic!("detached record must not settle"));
    let (error, attempt, returned_failure) = settle.into_parts();
    assert_eq!(error, PendingAttemptStateError::RecordNotRetained);
    assert_eq!(returned_failure, requested);
    let restore = attempt
        .restore(&mut pending)
        .err()
        .unwrap_or_else(|| panic!("detached record must not restore"));
    assert_eq!(
        restore.error(),
        PendingAttemptRestoreError::State(PendingAttemptStateError::RecordNotRetained)
    );
    let mut attempt = restore
        .into_attempt()
        .unwrap_or_else(|_failure| panic!("misuse should retain the exact attempt"));
    assert_eq!(
        attempt.transfer_state(),
        PendingRecordTransferState::Detached
    );
    assert!(Arc::ptr_eq(record.topic(), &expected_topic));
    attempt.restore_record(record).unwrap_or_else(|failure| {
        let (error, _record) = failure.into_parts();
        panic!("record should restore intact: {error:?}")
    });

    let local = attempt
        .settle_local(requested)
        .unwrap_or_else(|_failure| panic!("restored record should settle"));
    let (admission, job) = local.into_parts();
    assert!(Arc::ptr_eq(
        admission.into_record().topic(),
        &expected_topic
    ));
    job.dispatch_pending_notification_for_test();
    assert!(send.wait().is_err());
}

#[test]
fn committed_transfer_accepts_without_dropping_the_detached_record() {
    let mut pending = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = register(&mut pending, "committed");
    let send = registration.into_send();
    let mut attempt = take(&mut pending);
    let record = attempt
        .detach_record()
        .unwrap_or_else(|error| panic!("record should detach: {error:?}"));
    attempt
        .commit_record()
        .unwrap_or_else(|error| panic!("record transfer should commit: {error:?}"));
    let (mut completions, completion_id, observer) = observer();
    let accepted = attempt
        .accept(observer)
        .unwrap_or_else(|_failure| panic!("committed transfer should accept"));
    assert_eq!(record.topic().as_ref(), "committed");
    accepted
        .into_notification()
        .dispatch_pending_notification_for_test();
    assert_eq!(
        completions.publish(
            completion_id,
            ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
                DeliveryStatus::NotSent,
            )),
        ),
        Ok(())
    );
    assert!(send.wait().is_err());
    reclaim_and_stop(&mut completions, completion_id);
    drop(record);
}

#[test]
fn promotion_drop_cannot_restore_only_one_half() {
    let mut pending = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = register(&mut pending, "drop");
    let send = registration.into_send();
    let attempt = take(&mut pending);
    let cell = attempt.cell_for_test();

    drop(attempt);
    assert_eq!(pending.stats().records, 0);
    assert_eq!(pending.stats().retained_bytes, 0);
    assert!(matches!(
        cell.begin_promotion_for_test(),
        Err(PendingCellError::TransitionInProgress)
    ));
    drop(send);
    assert_eq!(pending.stats().notification_permits, 1);
}

fn register(pending: &mut PendingAdmissionRegistry, topic: &str) -> super::PendingSendRegistration {
    pending
        .register(
            ProducerRecord::new(
                Arc::from(topic),
                PartitionIndex::from_raw(0),
                1,
                None,
                Some(Bytes::from_static(b"value")),
            ),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(40), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"))
}

fn take(pending: &mut PendingAdmissionRegistry) -> super::PendingPromotionAttempt {
    pending
        .take_next(1)
        .unwrap_or_else(|error| panic!("promotion take should succeed: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("promotion attempt should exist"))
}

fn observer() -> (
    CompletionRegistry<ProducerCompletion>,
    crate::completion::CompletionId,
    ProducerDeliveryObserver,
) {
    let mut completions = CompletionRegistry::new(1, 1)
        .unwrap_or_else(|error| panic!("completion notifier should start: {error}"));
    let (id, observer) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("completion should reserve: {error}"));
    (
        completions,
        id,
        ProducerDeliveryObserver::from_completion(observer),
    )
}

fn complete_observer(
    completions: &mut CompletionRegistry<ProducerCompletion>,
    id: crate::completion::CompletionId,
    observer: ProducerDeliveryObserver,
) {
    assert_eq!(
        completions.publish(
            id,
            ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
                DeliveryStatus::NotSent,
            )),
        ),
        Ok(())
    );
    assert!(observer.wait().is_err());
    reclaim_and_stop(completions, id);
}

fn reclaim_and_stop(
    completions: &mut CompletionRegistry<ProducerCompletion>,
    expected: crate::completion::CompletionId,
) {
    for _attempt in 0..10_000 {
        match completions.next_reclaim() {
            Ok(Some(id)) => {
                assert_eq!(id, expected);
                assert_eq!(completions.finish_reclaim(id), Ok(ReclaimStatus::Reclaimed));
                let join = completions
                    .stop_notifier()
                    .unwrap_or_else(|error| panic!("completion notifier should stop: {error}"));
                assert_eq!(join.join_off_notifier(), Ok(()));
                return;
            }
            Ok(None) => std::thread::yield_now(),
            Err(error) => panic!("completion reclaim should remain connected: {error}"),
        }
    }
    panic!("completion should become reclaimable");
}
