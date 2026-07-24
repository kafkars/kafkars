//! Delivery certainty, deadline, and flush safety across producer retries.

use core::num::NonZeroI16;

use crate::{
    BatchTimerGeneration, Deadline, DeliveryStatus, Moment, ProducerAttemptFailureKind,
    ProducerBatchSuccess, ProducerBrokerFailure, ProducerBrokerFailureKind, ProducerCompletion,
    ProducerEffect, ProducerFailureKind, ProducerInput,
};

use super::scenario_support::retry::{
    fire_retry, has_retry, materialize_and_submit, next, submitted, transient_failure,
};

#[test]
fn possibly_sent_permanent_and_broker_failures_never_retry() {
    let (mut possibly_sent, _, execution) = submitted(3, 2, 30);
    let terminal = possibly_sent
        .apply(ProducerInput::TransportFailed {
            execution,
            now: Moment::from_tick(2),
            failure: ProducerAttemptFailureKind::LocalCapacity,
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("possibly-sent failure failed: {error}"));
    assert!(!has_retry(terminal.effects()));

    let (mut permanent, _, execution) = submitted(3, 2, 30);
    let terminal = permanent
        .apply(ProducerInput::TransportFailed {
            execution,
            now: Moment::from_tick(2),
            failure: ProducerAttemptFailureKind::Permanent,
            delivery: DeliveryStatus::NotSent,
        })
        .unwrap_or_else(|error| panic!("permanent failure failed: {error}"));
    assert!(!has_retry(terminal.effects()));

    let (mut broker, _, execution) = submitted(3, 2, 30);
    let terminal = broker
        .apply(ProducerInput::BrokerFailed {
            execution,
            failure: broker_retry_later(),
            delivery: DeliveryStatus::NotSent,
        })
        .unwrap_or_else(|error| panic!("broker failure failed: {error}"));
    assert!(!has_retry(terminal.effects()));
}

#[test]
fn original_deadline_caps_backoff_and_settles_before_another_attempt() {
    let (mut producer, _, first) = submitted(2, 5, 6);
    let retry = transient_failure(&mut producer, first, 2);
    assert!(matches!(
        retry.effects().last(),
        Some(ProducerEffect::ArmBatchTimer {
            deadline,
            ..
        }) if *deadline == Deadline::from_tick(6)
    ));
    let terminal = producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id: first.batch_id(),
            generation: BatchTimerGeneration::from_raw(2),
            now: Moment::from_tick(6),
        })
        .unwrap_or_else(|error| panic!("deadline timer failed: {error}"));
    assert!(!has_retry(terminal.effects()));
    assert!(matches!(
        terminal.effects().last(),
        Some(ProducerEffect::Complete {
            completion: ProducerCompletion::Failed(failure),
            ..
        }) if failure.kind() == ProducerFailureKind::DeadlineElapsed
            && failure.delivery() == DeliveryStatus::NotSent
    ));
}

#[test]
fn flush_waits_across_retry_and_completes_after_record_terminal() {
    let (mut producer, _, first) = submitted(1, 2, 30);
    let flush = producer
        .apply(ProducerInput::FlushRequested)
        .unwrap_or_else(|error| panic!("flush failed: {error}"));
    assert!(matches!(
        flush.effects(),
        [ProducerEffect::AcceptFlush { .. }]
    ));
    let retry = transient_failure(&mut producer, first, 2);
    assert!(!matches!(
        retry.effects().last(),
        Some(ProducerEffect::CompleteFlush { .. })
    ));
    let second = next(first);
    let ready = fire_retry(&mut producer, second, 2, 4);
    assert!(matches!(
        ready.effects(),
        [ProducerEffect::MaterializeBatch {
            execution,
            ..
        }] if *execution == second
    ));
    materialize_and_submit(&mut producer, second, 4);
    let terminal = producer
        .apply(ProducerInput::BrokerSucceeded {
            execution: second,
            success: ProducerBatchSuccess::new(7, None, None),
        })
        .unwrap_or_else(|error| panic!("success failed: {error}"));
    assert!(matches!(
        terminal.effects().last(),
        Some(ProducerEffect::CompleteFlush { .. })
    ));
}

#[test]
fn close_waits_across_retry_and_completes_after_record_terminal() {
    let (mut producer, _, first) = submitted(1, 2, 30);
    let close = producer
        .apply(ProducerInput::CloseRequested)
        .unwrap_or_else(|error| panic!("close failed: {error}"));
    assert!(matches!(
        close.effects(),
        [ProducerEffect::AcceptFlush { .. }]
    ));
    assert!(!producer.admission_is_open());
    let retry = transient_failure(&mut producer, first, 2);
    assert!(!matches!(
        retry.effects().last(),
        Some(ProducerEffect::CompleteFlush { .. })
    ));
    let second = next(first);
    fire_retry(&mut producer, second, 2, 4);
    materialize_and_submit(&mut producer, second, 4);
    let terminal = producer
        .apply(ProducerInput::BrokerSucceeded {
            execution: second,
            success: ProducerBatchSuccess::new(7, None, None),
        })
        .unwrap_or_else(|error| panic!("success failed: {error}"));
    assert!(matches!(
        terminal.effects().last(),
        Some(ProducerEffect::CompleteFlush { .. })
    ));
}

fn broker_retry_later() -> ProducerBrokerFailure {
    let code = NonZeroI16::new(20).unwrap_or_else(|| panic!("test code must be nonzero"));
    ProducerBrokerFailure::new(ProducerBrokerFailureKind::Retriable, code)
}
