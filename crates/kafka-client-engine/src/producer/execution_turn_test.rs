//! Host-turn materialization, waiting, expiry, and stale-work scenarios.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{
    ByteCount, Deadline, Moment, PartitionIndex, ProducerBatchPolicy, ProducerEffect,
    ProducerInput, TopicId,
};

use crate::{ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus};

use super::{
    AdmittedExplicit, ProducerHostInvariantError, ProducerRecord,
    admission_test::{admit, record},
    execution::PreparedExecutionError,
    host_limits_test::{start, valid_limits},
};

#[test]
fn immediate_batch_materializes_then_waits_without_fake_acceptance() {
    let mut host = start(ready_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(20),
        record("orders"),
    );

    assert_eq!(host.drive_prepared(Moment::from_tick(1), 1), Ok(1));
    let stats = host.stats();
    assert_eq!(stats.prepared_batches, 1);
    assert!(stats.prepared_bytes > 0);
    assert_eq!(stats.submission_deadlines, 0);
    assert!(matches!(
        host.pending_effects(),
        [ProducerEffect::SubmitProduce { .. }]
    ));

    assert_eq!(host.drive_prepared(Moment::from_tick(1), 1), Ok(1));
    assert_eq!(host.stats().prepared_batches, 1);
    assert_eq!(host.stats().submission_deadlines, 1);
    assert_eq!(host.stats().pending_effects, 0);
    assert_eq!(host.next_deadline(), Some(Deadline::from_tick(20)));
    drop(admitted);
}

#[test]
fn pre_driver_expiry_releases_both_byte_owners_and_publishes_not_sent() {
    let mut host = start(ready_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    assert_eq!(host.drive_prepared(Moment::from_tick(1), 2), Ok(1));
    assert_eq!(host.drive_prepared(Moment::from_tick(1), 2), Ok(1));

    assert_eq!(host.fire_due_submissions(Moment::from_tick(5), 1), Ok(1));
    let stats = host.stats();
    assert_eq!(stats.store.records, 0);
    assert_eq!(stats.store.bytes, 0);
    assert_eq!(stats.store.batches, 0);
    assert_eq!(stats.prepared_batches, 0);
    assert_eq!(stats.prepared_bytes, 0);
    assert_eq!(stats.submission_deadlines, 0);
    assert_eq!(stats.core_retained_bytes, ByteCount::new(0));
    assert_eq!(host.next_deadline(), None);
    assert_failure(admitted, ProducerDeliveryFailureKind::DeadlineElapsed);
}

#[test]
fn materialization_capacity_failure_is_a_core_owned_terminal_fact() {
    let mut limits = ready_limits();
    limits.encoded_byte_capacity = 1;
    let mut host = start(limits);
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(20),
        record("orders"),
    );

    assert_eq!(host.drive_prepared(Moment::from_tick(1), 1), Ok(1));
    let stats = host.stats();
    assert_eq!(stats.store.records, 0);
    assert_eq!(stats.prepared_batches, 0);
    assert_eq!(stats.prepared_bytes, 0);
    assert_eq!(stats.submission_deadlines, 0);
    assert_eq!(stats.pending_effects, 0);
    assert_failure(admitted, ProducerDeliveryFailureKind::MaterializationFailed);
}

#[test]
fn semantic_partition_encoding_failure_settles_without_poisoning_host() {
    let mut host = start(ready_limits());
    let record = ProducerRecord::new(
        Arc::from("orders"),
        PartitionIndex::from_raw(i32::MAX as u32 + 1),
        10,
        None,
        Some(Bytes::from_static(b"value")),
    );
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(20),
        record,
    );

    assert_eq!(host.drive_prepared(Moment::from_tick(1), 1), Ok(1));
    assert!(host.stats().healthy);
    assert_eq!(host.stats().store.records, 0);
    assert_failure(admitted, ProducerDeliveryFailureKind::MaterializationFailed);
}

#[test]
fn route_disagreement_poisons_the_host_closed() {
    let mut host = start(ready_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(20),
        record("orders"),
    );
    assert_eq!(host.drive_prepared(Moment::from_tick(1), 1), Ok(1));
    let effect = host
        .pending_effects
        .pop()
        .unwrap_or_else(|| panic!("submission effect should be pending"));
    let ProducerEffect::SubmitProduce {
        batch_id,
        deadline_operation_id,
        deadline,
        topic_id,
        partition,
        acknowledgements,
    } = effect
    else {
        panic!("materialized batch should request submission")
    };
    host.pending_effects.push(ProducerEffect::SubmitProduce {
        batch_id,
        deadline_operation_id,
        deadline,
        topic_id: TopicId::from_raw(topic_id.get() + 1),
        partition,
        acknowledgements,
    });

    let result = host.drive_prepared(Moment::from_tick(1), 1);
    assert!(matches!(
        result,
        Err(ProducerHostInvariantError::Prepared(
            PreparedExecutionError::RouteMismatch { .. }
        ))
    ));
    assert!(!host.stats().healthy);
    drop(admitted);
}

#[test]
fn terminal_release_cancels_submission_that_never_started() {
    let mut host = start(ready_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    assert_eq!(host.drive_prepared(Moment::from_tick(1), 1), Ok(1));
    assert_eq!(host.stats().prepared_batches, 1);
    assert!(matches!(
        host.pending_effects(),
        [ProducerEffect::SubmitProduce { .. }]
    ));
    let transition = host
        .core
        .apply(ProducerInput::DeadlineElapsed {
            operation_id: admitted.operation_id(),
            now: Moment::from_tick(5),
        })
        .unwrap_or_else(|error| panic!("deadline should settle pending materialization: {error}"));
    host.interpret_transition(Moment::from_tick(5), transition)
        .unwrap_or_else(|error| panic!("terminal effects should execute: {error}"));

    assert_eq!(host.stats().pending_effects, 0);
    assert_eq!(host.stats().prepared_batches, 0);
    assert_eq!(host.drive_prepared(Moment::from_tick(5), 1), Ok(0));
    assert_failure(admitted, ProducerDeliveryFailureKind::DeadlineElapsed);
}

#[test]
fn due_submission_work_is_fifo_and_bounded_per_turn() {
    let mut host = start(ready_limits());
    let first = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let second = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("payments"),
    );
    assert_eq!(host.drive_prepared(Moment::from_tick(1), 2), Ok(2));
    assert_eq!(host.drive_prepared(Moment::from_tick(1), 2), Ok(2));

    assert_eq!(host.fire_due_submissions(Moment::from_tick(5), 1), Ok(1));
    assert_eq!(host.stats().submission_deadlines, 1);
    assert_eq!(host.stats().store.records, 1);
    assert_failure(first, ProducerDeliveryFailureKind::DeadlineElapsed);

    assert_eq!(host.fire_due_submissions(Moment::from_tick(5), 1), Ok(1));
    assert_eq!(host.stats().submission_deadlines, 0);
    assert_eq!(host.stats().store.records, 0);
    assert_failure(second, ProducerDeliveryFailureKind::DeadlineElapsed);
}

fn ready_limits() -> super::ProducerHostLimits {
    let Ok(policy) = ProducerBatchPolicy::try_new(1, ByteCount::new(u64::MAX), 10) else {
        panic!("ready policy should be valid")
    };
    let mut limits = valid_limits();
    limits.batch_policy = policy;
    limits
}

fn assert_failure(admitted: AdmittedExplicit, expected: ProducerDeliveryFailureKind) {
    let Err(ProducerDeliveryError::Failed(failure)) = admitted.into_delivery_observer().wait()
    else {
        panic!("producer operation should fail");
    };
    assert_eq!(failure.kind(), expected);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}
