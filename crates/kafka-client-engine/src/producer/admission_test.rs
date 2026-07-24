//! Atomic reservation, rollback, and observer scenarios for producer admission.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{
    AdmissionRejection, ByteCount, Deadline, Moment, PartitionIndex, ProducerBatchPolicy,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ProducerHost, ProducerHostInvariantError, ProducerRecord, ProducerRejectionReason,
    ProducerStoreError,
    admission::{AdmittedExplicit, ProducerAdmissionFailure, RejectedExplicit},
    host_limits_test::{start, valid_limits},
};

#[test]
fn non_ready_admission_synchronizes_core_store_timer_and_observer() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(10),
        Deadline::from_tick(90),
        record("orders"),
    );

    assert_eq!(admitted.operation_id().get(), 1);
    let stats = host.stats();
    assert_eq!(stats.store.records, 1);
    assert_eq!(stats.store.bytes, 7);
    assert_eq!(stats.store.batches, 1);
    assert_eq!(stats.core_retained_bytes, ByteCount::new(7));
    assert_eq!(stats.core_completion_slots, 1);
    assert_eq!(stats.active_timers, 1);
    assert_eq!(stats.pending_effects, 0);
    assert_eq!(host.next_deadline(), Some(Deadline::from_tick(90)));
    assert!(stats.healthy);
    drop(admitted);
}

#[test]
fn completion_rejection_returns_the_exact_record_before_store_reservation() {
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_capacity = 1;
    limits.timer_capacity = 1;
    limits.batch_policy = single_record_with_linger();
    let mut host = start(limits);
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(20),
        record("first"),
    );
    let topic: Arc<str> = Arc::from("second");
    let rejected = reject_result(host.try_admit_explicit(
        Moment::from_tick(0),
        operation_deadline(20),
        record_from(Arc::clone(&topic)),
    ));

    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::Completion(CompletionRegistryError::Full)
    );
    assert!(Arc::ptr_eq(rejected.into_record().topic(), &topic));
    assert_eq!(host.stats().store.records, 1);
    drop(admitted);
}

#[test]
fn byte_rejection_rolls_back_the_reserved_completion_slot() {
    let mut limits = valid_limits();
    limits.retained_bytes = 6;
    let mut host = start(limits);
    let topic: Arc<str> = Arc::from("orders");
    let rejected = reject_result(host.try_admit_explicit(
        Moment::from_tick(0),
        operation_deadline(20),
        record_from(Arc::clone(&topic)),
    ));

    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::Store(ProducerStoreError::ByteCapacity)
    );
    assert!(Arc::ptr_eq(rejected.into_record().topic(), &topic));
    let first = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(20),
        record("a"),
    );
    let second = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(20),
        record("b"),
    );
    assert_eq!(host.stats().core_completion_slots, 2);
    drop((first, second));
}

#[test]
fn elapsed_deadline_rolls_back_both_reservations_and_preserves_record() {
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_capacity = 1;
    limits.timer_capacity = 1;
    limits.batch_policy = single_record_with_linger();
    let mut host = start(limits);
    let topic: Arc<str> = Arc::from("orders");
    let rejected = reject_result(host.try_admit_explicit(
        Moment::from_tick(10),
        operation_deadline(10),
        record_from(Arc::clone(&topic)),
    ));

    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::Core(AdmissionRejection::DeadlineElapsed)
    );
    let returned = rejected.into_record();
    assert!(Arc::ptr_eq(returned.topic(), &topic));
    assert_eq!(host.stats().store.records, 0);
    assert_eq!(host.stats().core_completion_slots, 0);
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(10),
        returned,
    );
    drop(admitted);
}

#[test]
fn deadline_overflow_is_a_core_rejection_with_full_rollback() {
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_capacity = 1;
    limits.timer_capacity = 1;
    limits.batch_policy = single_record_with_linger();
    let mut host = start(limits);
    let rejected = reject_result(host.try_admit_explicit(
        Moment::from_tick(u64::MAX),
        operation_deadline(u64::MAX),
        record("orders"),
    ));

    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::Core(AdmissionRejection::DeadlineOverflow)
    );
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(20),
        rejected.into_record(),
    );
    drop(admitted);
}

#[test]
fn post_acceptance_fault_poisons_host_and_preserves_the_next_record() {
    let mut host = start(valid_limits());
    host.inject_post_acceptance_fault(ProducerHostInvariantError::MissingAdmissionIdentity);
    let first = host.try_admit_explicit(
        Moment::from_tick(0),
        operation_deadline(20),
        record("first"),
    );
    assert!(matches!(
        first,
        Err(ProducerAdmissionFailure::AcceptedInvariant(_))
    ));
    assert!(!host.stats().healthy);

    let topic: Arc<str> = Arc::from("second");
    let rejected = reject_result(host.try_admit_explicit(
        Moment::from_tick(0),
        operation_deadline(20),
        record_from(Arc::clone(&topic)),
    ));
    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::HostPoisoned(ProducerHostInvariantError::MissingAdmissionIdentity)
    );
    assert!(Arc::ptr_eq(rejected.into_record().topic(), &topic));
}

pub(super) fn admit(
    host: &mut ProducerHost,
    now: Moment,
    deadline: Deadline,
    record: ProducerRecord,
) -> AdmittedExplicit {
    let admitted = match host.try_admit_explicit(
        now,
        OperationDeadline::from_parts_for_test(deadline, Instant::now()),
        record,
    ) {
        Ok(admitted) => admitted,
        Err(error) => panic!("producer admission should succeed: {error:?}"),
    };
    super::test_identity::acquire_host_if_pending(host, now);
    admitted
}

fn operation_deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}

pub(super) fn record(topic: &str) -> ProducerRecord {
    record_from(Arc::from(topic))
}

fn record_from(topic: Arc<str>) -> ProducerRecord {
    ProducerRecord::new(
        topic,
        PartitionIndex::from_raw(0),
        10,
        None,
        Some(Bytes::from_static(b"x")),
    )
}

fn reject_result(result: Result<AdmittedExplicit, ProducerAdmissionFailure>) -> RejectedExplicit {
    match result {
        Err(ProducerAdmissionFailure::Rejected(rejected)) => rejected,
        Err(ProducerAdmissionFailure::Invariant(error)) => {
            panic!("admission violated a host invariant: {error:?}")
        }
        Err(ProducerAdmissionFailure::AcceptedInvariant(error)) => {
            panic!("admission violated an accepted host invariant: {error:?}")
        }
        Ok(_admitted) => panic!("producer admission should reject"),
    }
}

fn single_record_with_linger() -> ProducerBatchPolicy {
    match ProducerBatchPolicy::try_new(1, ByteCount::new(u64::MAX), 1) {
        Ok(policy) => policy,
        Err(error) => panic!("single-record test policy should be valid: {error}"),
    }
}
