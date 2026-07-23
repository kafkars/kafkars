//! Atomic waiting registration, FIFO precedence, byte bounds, and wake scenarios.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::{io, time::Instant};

use bytes::Bytes;
use kafka_client_core::{ByteCount, Deadline, Moment, PartitionIndex, ProducerBatchPolicy};

use super::{
    ProducerAdmissionPort, ProducerPortAdmissionError, ProducerPortRejectionReason,
    ProducerShardOwner, ProducerShardWake, ProducerShardWakeError, ProducerWaitingStart,
};
use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerHostLimits, ProducerRecord,
        host_limits_test::{start, valid_limits},
        pending::PendingAdmissionRejectionReason,
    },
};

#[test]
fn empty_fifo_admits_immediately_and_wakes_after_releasing_the_shard() {
    let (owner, port, wake) = setup(valid_limits());
    let outcome = port.start_waiting_explicit(now(), deadline(90), record("orders"));
    let ProducerWaitingStart::Accepted(accepted) = outcome else {
        panic!("empty FIFO with capacity should admit")
    };
    let (observer, operation_id, fault) = accepted.into_parts();

    assert_eq!(
        operation_id.map(kafka_client_core::OperationId::get),
        Some(1)
    );
    assert!(fault.is_ok());
    assert_eq!(wake.count(), 1);
    assert!(wake.every_wake_observed_unlocked());
    assert_eq!(host(&owner).shard_stats().pending.records, 0);
    drop(observer);
}

#[test]
fn capacity_rejection_registers_exact_pending_ownership_and_wakes_once() {
    let (owner, port, wake) = setup(single_accepted_limits());
    let first = port
        .try_admit_explicit(now(), deadline(80), record("first"))
        .unwrap_or_else(|error| panic!("first record should admit: {error:?}"));
    let original = deadline(91);
    let outcome = port.start_waiting_explicit(now(), original, record("second"));
    let ProducerWaitingStart::Pending(registration) = outcome else {
        panic!("second record should enter bounded pending ownership")
    };
    let stats = host(&owner).shard_stats();

    assert_eq!(stats.host.core_completion_slots, 1);
    assert_eq!(stats.pending.records, 1);
    assert_eq!(stats.pending.notification_permits, 1);
    assert_eq!(wake.count(), 2);
    assert!(wake.every_wake_observed_unlocked());
    drop((first, registration.into_send()));
}

#[test]
fn older_pending_entry_fences_nonblocking_try_send_even_with_core_capacity() {
    let (owner, port, wake) = setup(valid_limits());
    let pending = host(&owner)
        .register_pending(record("older"), deadline(90))
        .unwrap_or_else(|error| panic!("older pending record should register: {error:?}"));

    let result = port.try_admit_explicit(now(), deadline(90), record("newer"));
    let Err(ProducerPortAdmissionError::Rejected(rejected)) = result else {
        panic!("try_send must not bypass older pending ownership")
    };
    assert_eq!(
        rejected.reason(),
        ProducerPortRejectionReason::PendingPrecedence
    );
    assert_eq!(rejected.into_record().topic().as_ref(), "newer");
    assert_eq!(host(&owner).shard_stats().host.core_completion_slots, 0);
    assert_eq!(wake.count(), 0);
    drop(pending.into_send());
}

#[test]
fn full_pending_population_returns_exact_backpressure_owner_without_wake() {
    let mut limits = valid_limits();
    limits.pending_record_capacity = 1;
    limits.pending_notification_capacity = 1;
    limits.notification_capacity = limits.completion_capacity + 1;
    let (owner, port, wake) = setup(limits);
    let first = host(&owner)
        .register_pending(record("first"), deadline(90))
        .unwrap_or_else(|error| panic!("first pending record should register: {error:?}"));

    let outcome = port.start_waiting_explicit(now(), deadline(90), record("second"));
    let ProducerWaitingStart::PendingRejected(rejected) = outcome else {
        panic!("full pending population should return its exact record")
    };
    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::CountCapacity
    );
    assert_eq!(rejected.into_record().topic().as_ref(), "second");
    assert_eq!(wake.count(), 0);
    drop(first.into_send());
}

#[test]
fn aggregate_byte_ceiling_rejects_pending_fallback_without_wake() {
    let mut limits = valid_limits();
    limits.retained_bytes = 6;
    let (owner, port, wake) = setup(limits);

    let outcome = port.start_waiting_explicit(now(), deadline(90), record("orders"));
    let ProducerWaitingStart::PendingRejected(rejected) = outcome else {
        panic!("aggregate byte ceiling should reject pending fallback")
    };
    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::ByteCapacity
    );
    assert_eq!(rejected.into_record().topic().as_ref(), "orders");
    assert_eq!(host(&owner).shard_stats().aggregate_retained_bytes, 0);
    assert_eq!(wake.count(), 0);
}

#[test]
fn pending_wake_failure_cannot_revoke_registered_ownership() {
    let wake = Arc::new(FailingWake::default());
    let owner = ProducerShardOwner::new(start(single_accepted_limits()), Arc::clone(&wake));
    let port = owner.admission_port();
    let first = port
        .try_admit_explicit(now(), deadline(80), record("first"))
        .unwrap_or_else(|error| panic!("first record should commit: {error:?}"));
    let outcome = port.start_waiting_explicit(now(), deadline(90), record("second"));
    let ProducerWaitingStart::Pending(registration) = outcome else {
        panic!("failed wake must preserve pending registration")
    };

    let stats = host(&owner).shard_stats();
    assert_eq!(stats.host.core_completion_slots, 1);
    assert_eq!(stats.pending.records, 1);
    assert_eq!(stats.pending.notification_permits, 1);
    assert_eq!(wake.count.load(Ordering::Acquire), 2);
    drop((first, registration.into_send()));
}

fn setup(
    limits: ProducerHostLimits,
) -> (
    ProducerShardOwner,
    ProducerAdmissionPort,
    Arc<LockWitnessWake>,
) {
    let wake = Arc::new(LockWitnessWake::default());
    let owner = ProducerShardOwner::new(start(limits), Arc::clone(&wake));
    let port = owner.admission_port();
    wake.install(port.clone());
    (owner, port, wake)
}

fn host(owner: &ProducerShardOwner) -> std::sync::MutexGuard<'_, super::data::ProducerShardData> {
    owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should own the shard: {error:?}"))
}

fn single_accepted_limits() -> ProducerHostLimits {
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_capacity = 1;
    limits.timer_capacity = 1;
    limits.notification_capacity = 1 + limits.pending_record_capacity;
    limits.batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(64), 100)
        .unwrap_or_else(|error| panic!("single-record policy should validate: {error:?}"));
    limits
}

fn record(topic: &str) -> ProducerRecord {
    ProducerRecord::new(
        Arc::from(topic),
        PartitionIndex::from_raw(0),
        1,
        None,
        Some(Bytes::from_static(b"x")),
    )
}

const fn now() -> Moment {
    Moment::from_tick(10)
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}

#[derive(Default)]
struct LockWitnessWake {
    port: Mutex<Option<ProducerAdmissionPort>>,
    count: AtomicUsize,
    all_unlocked: AtomicBool,
}

impl LockWitnessWake {
    fn install(&self, port: ProducerAdmissionPort) {
        *self
            .port
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(port);
        self.all_unlocked.store(true, Ordering::Release);
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    fn every_wake_observed_unlocked(&self) -> bool {
        self.all_unlocked.load(Ordering::Acquire)
    }
}

impl ProducerShardWake for LockWitnessWake {
    fn wake(&self) -> Result<(), ProducerShardWakeError> {
        self.count.fetch_add(1, Ordering::AcqRel);
        let unlocked = self
            .port
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(ProducerAdmissionPort::shard_lock_available_for_test);
        self.all_unlocked.fetch_and(unlocked, Ordering::AcqRel);
        Ok(())
    }
}

#[derive(Default)]
struct FailingWake {
    count: AtomicUsize,
}

impl ProducerShardWake for FailingWake {
    fn wake(&self) -> Result<(), ProducerShardWakeError> {
        self.count.fetch_add(1, Ordering::AcqRel);
        Err(ProducerShardWakeError::from_io(io::Error::other(
            "intentional waiting wake failure",
        )))
    }
}
