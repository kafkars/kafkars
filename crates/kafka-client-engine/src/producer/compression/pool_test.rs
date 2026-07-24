//! Bounded queue, byte, deadline, cancellation, and shutdown scenarios.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, CompressionPolicy, Deadline, Moment,
    OperationId,
};

use super::{CompressionJob, CompressionSchedule, CompressionWorkerLimits, CompressionWorkers};
use crate::producer::{
    batch_store::MaterializationAttempt,
    ingress::{ProducerShardWake, ProducerShardWakeError},
    materialization::{MaterializationBatch, MaterializationRecord},
};

#[test]
fn one_bounded_slot_returns_the_second_linear_job() {
    let wake = Arc::new(CountingWake::default());
    let mut workers = CompressionWorkers::start(
        CompressionWorkerLimits {
            workers: 1,
            jobs: 1,
            bytes: 4_096,
        },
        &wake,
    )
    .unwrap_or_else(|error| panic!("pool start failed: {error}"));

    assert!(matches!(
        workers.try_submit(job(1), OperationId::from_raw(1), Deadline::from_tick(10)),
        CompressionSchedule::Accepted
    ));
    assert!(matches!(
        workers.try_submit(job(2), OperationId::from_raw(2), Deadline::from_tick(20)),
        CompressionSchedule::Full(_)
    ));
    assert_eq!(workers.retained_jobs(), 1);
    workers.shutdown();
    assert_eq!(workers.retained_jobs(), 0);
    assert_eq!(workers.retained_bytes(), 0);
}

#[test]
fn due_deadline_fences_the_exact_late_worker_result() {
    let wake = Arc::new(CountingWake::default());
    let mut workers = CompressionWorkers::start(
        CompressionWorkerLimits {
            workers: 1,
            jobs: 1,
            bytes: 4_096,
        },
        &wake,
    )
    .unwrap_or_else(|error| panic!("pool start failed: {error}"));
    let operation_id = OperationId::from_raw(7);
    let execution = execution(1);
    assert!(matches!(
        workers.try_submit(job(1), operation_id, Deadline::from_tick(5)),
        CompressionSchedule::Accepted
    ));

    assert_eq!(
        workers.drain_due(Moment::from_tick(5), 1),
        vec![kafka_client_core::ProducerInput::DeadlineElapsed {
            operation_id,
            now: Moment::from_tick(5),
        }]
    );
    let result = workers
        .complete_with_timeout(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("worker result failed: {error:?}"))
        .unwrap_or_else(|| panic!("worker did not return"));
    assert_eq!(result.0.execution(), execution);
    assert!(result.1);
    assert!(wake.count.load(Ordering::Acquire) > 0);
}

#[test]
fn worker_materializes_and_wakes_away_from_the_host_thread() {
    let wake = Arc::new(CountingWake::default());
    let mut workers = CompressionWorkers::start(
        CompressionWorkerLimits {
            workers: 1,
            jobs: 1,
            bytes: 4_096,
        },
        &wake,
    )
    .unwrap_or_else(|error| panic!("pool start failed: {error}"));
    assert!(matches!(
        workers.try_submit(job(1), OperationId::from_raw(8), Deadline::from_tick(50)),
        CompressionSchedule::Accepted
    ));
    let (_, cancelled) = workers
        .complete_with_timeout(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("worker result failed: {error:?}"))
        .unwrap_or_else(|| panic!("worker did not return"));

    assert!(!cancelled);
    assert!(wake.count.load(Ordering::Acquire) > 0);
    assert_ne!(
        *wake
            .thread_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        Some(thread::current().id())
    );
}

#[test]
fn explicit_cancellation_fences_a_completed_but_unapplied_generation() {
    let mut workers = CompressionWorkers::start(
        CompressionWorkerLimits {
            workers: 1,
            jobs: 1,
            bytes: 4_096,
        },
        &Arc::new(CountingWake::default()),
    )
    .unwrap_or_else(|error| panic!("pool start failed: {error}"));
    let execution = execution(1);
    assert!(matches!(
        workers.try_submit(job(1), OperationId::from_raw(9), Deadline::from_tick(50)),
        CompressionSchedule::Accepted
    ));
    workers.cancel(execution);

    let (_, cancelled) = workers
        .complete_with_timeout(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("worker result failed: {error:?}"))
        .unwrap_or_else(|| panic!("worker did not return"));
    assert!(cancelled);
    assert!(!workers.contains(execution));
}

#[test]
fn shutdown_joins_every_owned_native_worker() {
    let mut workers = CompressionWorkers::start(
        CompressionWorkerLimits {
            workers: 2,
            jobs: 1,
            bytes: 4_096,
        },
        &Arc::new(CountingWake::default()),
    )
    .unwrap_or_else(|error| panic!("pool start failed: {error}"));
    assert_eq!(workers.worker_count(), 2);

    workers.shutdown();

    assert_eq!(workers.worker_count(), 0);
}

fn job(generation: u64) -> CompressionJob {
    let execution = execution(generation);
    let input = MaterializationBatch::try_for_test(
        "orders",
        0,
        vec![MaterializationRecord::new(
            1,
            None,
            Some(Bytes::from_static(b"compress me")),
            Vec::new(),
        )],
        1_024,
    )
    .unwrap_or_else(|| panic!("test materialization must be representable"));
    CompressionJob::new(
        MaterializationAttempt::for_test(execution),
        input,
        CompressionPolicy::Gzip,
    )
    .unwrap_or_else(|_| panic!("test reservation must be representable"))
}

fn execution(generation: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(1),
        BatchExecutionGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("test generation must be nonzero")),
    )
}

#[derive(Default)]
struct CountingWake {
    count: AtomicUsize,
    thread_id: Mutex<Option<thread::ThreadId>>,
}

impl ProducerShardWake for CountingWake {
    fn wake(&self) -> Result<(), ProducerShardWakeError> {
        self.count.fetch_add(1, Ordering::AcqRel);
        *self
            .thread_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(thread::current().id());
        Ok(())
    }
}
