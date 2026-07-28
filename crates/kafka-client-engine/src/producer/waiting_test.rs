//! Saturation, cancellation, timeout, close, and shutdown tests for producer waiting.

#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail immediately on invalid fixtures or ownership outcomes"
)]

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use kafka_client_core::{
    ByteCount, Deadline, FlushId, Moment, OperationId, ProducerBatchPolicy,
    ProducerCancellationOutcome,
};

use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    clock::OperationDeadline,
};

use super::{
    host_limits_test::{start, valid_limits},
    ingress::{ProducerShardOwner, ProducerShardWake, ProducerShardWakeError},
    reclaim::CompletionReclaimOutcome,
    terminal_backlog::{ProducerTerminalOwner, RetainedTerminal},
};

struct TestWake;

impl ProducerShardWake for TestWake {
    fn wake(&self) -> Result<(), ProducerShardWakeError> {
        Ok(())
    }
}

#[test]
fn saturated_active_partition_still_accepts_and_promotes_one_waiter() {
    let mut limits = one_active_limits();
    limits.waiting_record_capacity = 2;
    limits.waiting_byte_capacity = 128;
    let mut host = start(limits);
    let active = super::admission_test::admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(50),
        super::admission_test::record("active"),
    );
    let waiting = host
        .try_admit_waiting(
            Moment::from_tick(1),
            deadline(40),
            super::admission_test::record("waiting"),
        )
        .unwrap_or_else(|_| panic!("separate waiting capacity must accept"));
    assert_eq!(host.stats().store.records, 1);
    assert_eq!(host.stats().waiting.records, 1);
    assert!(host.drive_waiting(Moment::from_tick(1), 1).unwrap().blocked);

    let operation_id = active.operation_id();
    let active_observer = active.into_delivery_observer();
    let cancelled = host
        .try_cancel_operation(operation_id)
        .unwrap_or_else(|error| panic!("active cancellation failed: {error:?}"));
    assert_eq!(
        cancelled.outcome(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    let active_error = active_observer
        .wait()
        .expect_err("cancelled active operation should fail");
    assert_not_sent(active_error, ProducerDeliveryFailureKind::Cancelled);
    host.reclaim_one(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("active reclaim failed: {error}"));
    let progress = host
        .drive_waiting(Moment::from_tick(2), 1)
        .unwrap_or_else(|error| panic!("waiter promotion failed: {error}"));
    assert_eq!(progress.progressed, 1);
    assert!(!progress.blocked);
    assert_eq!(host.stats().waiting.records, 0);
    assert_eq!(host.stats().waiting.bytes.get(), 0);
    assert_eq!(host.stats().store.records, 1);

    let (_id, observer, _token) = waiting.into_parts();
    host.execution_unavailable(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("shutdown settlement failed: {error}"));
    let error = observer
        .wait()
        .expect_err("promoted operation should stop terminally");
    assert_not_sent(error, ProducerDeliveryFailureKind::ExecutionUnavailable);
}

#[test]
fn timeout_releases_waiting_count_and_bytes_before_terminal_reclaim() {
    let limits = one_active_limits();
    let mut host = start(limits);
    let active = super::admission_test::admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(50),
        super::admission_test::record("active"),
    );
    let waiting = host
        .try_admit_waiting(
            Moment::from_tick(1),
            deadline(2),
            super::admission_test::record("timeout"),
        )
        .unwrap_or_else(|_| panic!("waiter should be retained"));
    let waiting_operation = OperationId::from_raw(
        active
            .operation_id()
            .get()
            .checked_add(1)
            .unwrap_or_else(|| panic!("test operation identity must advance")),
    );
    let completion_id = host
        .bindings
        .completion(waiting_operation)
        .unwrap_or_else(|| panic!("waiting operation must own its exact completion"));
    let (_id, observer, _token) = waiting.into_parts();

    host.drive_waiting(Moment::from_tick(2), 1)
        .unwrap_or_else(|error| panic!("timeout turn failed: {error}"));
    assert_eq!(host.stats().waiting.records, 0);
    assert_eq!(host.stats().waiting.bytes.get(), 0);
    let error = observer.wait().expect_err("waiter deadline should fail");
    assert_not_sent(error, ProducerDeliveryFailureKind::DeadlineElapsed);
    assert_eq!(host.stats().waiting.terminal_bindings, 1);
    assert_eq!(host.stats().core_completion_slots, 2);
    assert_eq!(
        host.reclaim_one(Moment::from_tick(2)),
        Ok(Some(CompletionReclaimOutcome::Reclaimed {
            owner: ProducerTerminalOwner::Record(waiting_operation),
            completion_id,
        }))
    );
    assert_eq!(host.stats().waiting.terminal_bindings, 0);
    assert_eq!(host.bindings.completion(waiting_operation), None);
    assert_eq!(host.stats().core_completion_slots, 1);
    assert_eq!(host.reclaim_one(Moment::from_tick(2)), Ok(None));
    drop(active);
}

#[test]
fn waiting_deadline_publication_stays_before_its_flush_terminal() {
    let mut host = start(one_active_limits());
    let waiting = host
        .try_admit_waiting(
            Moment::from_tick(0),
            deadline(2),
            super::admission_test::record("timeout"),
        )
        .unwrap_or_else(|_| panic!("waiter should be retained"));
    let flush = host
        .try_admit_flush(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("flush should be accepted: {error:?}"));
    host.inject_terminal_publish_fault(
        crate::completion::CompletionRegistryError::NotificationBackpressure,
    );

    host.drive_waiting(Moment::from_tick(2), 1)
        .unwrap_or_else(|error| panic!("timeout turn failed: {error}"));
    assert_eq!(host.stats().terminal_backlog, 2);
    assert_eq!(
        host.terminal_front().map(RetainedTerminal::owner),
        Some(ProducerTerminalOwner::Record(OperationId::from_raw(1)))
    );
    assert_eq!(
        host.terminal_back().map(RetainedTerminal::owner),
        Some(ProducerTerminalOwner::Flush(FlushId::from_raw(1)))
    );

    assert_eq!(host.retry_terminal_backlog(1), Ok(1));
    let (_id, observer, _token) = waiting.into_parts();
    let error = observer.wait().expect_err("waiter deadline should fail");
    assert_not_sent(error, ProducerDeliveryFailureKind::DeadlineElapsed);
    let mut flush = flush.into_flush_observer();
    let waker = Waker::noop();
    assert_eq!(
        Pin::new(&mut flush).poll(&mut Context::from_waker(waker)),
        Poll::Pending
    );
    assert_eq!(host.retry_terminal_backlog(1), Ok(1));
    assert_eq!(flush.wait(), Ok(()));
}

#[test]
fn dropping_waiting_observer_requests_exact_not_sent_cancellation() {
    let limits = one_active_limits();
    let mut host = start(limits);
    let active = super::admission_test::admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(50),
        super::admission_test::record("active"),
    );
    let owner = ProducerShardOwner::new(host, Arc::new(TestWake));
    let port = owner.admission_port();
    let accepted = port
        .admit_waiting(
            Moment::from_tick(1),
            deadline(40),
            super::admission_test::record("drop"),
        )
        .unwrap_or_else(|_| panic!("waiting admission should succeed"));
    let (observer, _operation, _fault) = accepted.into_parts();
    drop(observer);

    let mut data = owner.try_data().expect("host turn lock");
    data.turn(
        Moment::from_tick(2),
        super::host_turn::ProducerTurnBudget::try_new(1, 1, 1, 1, 1, 1)
            .expect("nonzero turn budget"),
    )
    .unwrap_or_else(|error| panic!("abandonment turn failed: {error}"));
    let stats = data.shard_stats().host;
    assert_eq!(stats.waiting.records, 0);
    assert_eq!(stats.waiting.bytes.get(), 0);
    drop(active);
}

#[test]
fn close_and_shutdown_fence_promotion_and_settle_not_sent() {
    for shutdown in [false, true] {
        let limits = one_active_limits();
        let mut host = start(limits);
        let active = super::admission_test::admit(
            &mut host,
            Moment::from_tick(0),
            Deadline::from_tick(50),
            super::admission_test::record("active"),
        );
        let waiting = host
            .try_admit_waiting(
                Moment::from_tick(1),
                deadline(40),
                super::admission_test::record("fenced"),
            )
            .unwrap_or_else(|_| panic!("waiter should be retained"));
        let (_id, observer, _token) = waiting.into_parts();
        if shutdown {
            host.execution_unavailable(Moment::from_tick(2))
                .unwrap_or_else(|error| panic!("shutdown settlement failed: {error}"));
        } else {
            host.close_admission();
            host.drive_waiting(Moment::from_tick(2), 1)
                .unwrap_or_else(|error| panic!("close settlement failed: {error}"));
        }
        let error = observer.wait().expect_err("fenced waiter should fail");
        assert_not_sent(error, ProducerDeliveryFailureKind::ExecutionUnavailable);
        assert_eq!(host.stats().waiting.records, 0);
        assert_eq!(host.stats().waiting.bytes.get(), 0);
        drop(active);
    }
}

fn deadline(tick: u64) -> OperationDeadline {
    let transport = Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap_or_else(|| panic!("test transport deadline should fit"));
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), transport)
}

fn one_active_limits() -> super::ProducerHostLimits {
    let mut limits = valid_limits();
    limits.completion_capacity = 1;
    limits.record_capacity = 1;
    limits.batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(64), 100)
        .expect("single-record test policy");
    limits
}

fn assert_not_sent(error: ProducerDeliveryError, expected: ProducerDeliveryFailureKind) {
    let ProducerDeliveryError::Failed(failure) = error else {
        panic!("expected semantic producer failure")
    };
    assert_eq!(failure.kind(), expected);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}
