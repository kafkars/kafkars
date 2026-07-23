//! Producer-host execution-stop and emergency fallback scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_core::{
    Deadline, DeliveryStatus, Moment, OperationId, PartitionIndex, ProducerCompletion,
    ProducerEffect, ProducerFailure,
};

use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    completion::CompletionRegistryError,
    producer::{
        ProducerHostInvariantError,
        admission_test::{admit, record},
        binding::OperationBindingError,
        host_limits_test::{start, valid_limits},
        terminal_backlog::RejectedTerminal,
    },
};

#[test]
fn deterministic_execution_stop_settles_pre_driver_work_not_sent() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );

    host.execution_unavailable(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("execution stop should settle: {error}"));

    assert_failure(
        admitted.into_delivery_observer().wait(),
        ProducerDeliveryStatus::NotSent,
    );
    assert_eq!(host.unsettled_completions(), 0);
}

#[test]
fn damaged_interpretation_still_settles_observers_conservatively() {
    let mut host = start(valid_limits());
    let payload_dropped = Arc::new(AtomicBool::new(false));
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        probed_record(Arc::clone(&payload_dropped)),
    );
    let mut observer = admitted.into_delivery_observer();
    let waker_called = Arc::new(AtomicBool::new(false));
    let released_before_wake = Arc::new(AtomicBool::new(false));
    let waker = Waker::from(Arc::new(ReleaseWitness {
        payload_dropped: Arc::clone(&payload_dropped),
        waker_called: Arc::clone(&waker_called),
        released_before_wake: Arc::clone(&released_before_wake),
    }));
    assert_eq!(
        Pin::new(&mut observer).poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    );
    let retained = host.stats();
    assert_eq!(retained.store.records, 1);
    assert!(retained.store.bytes > 0);
    assert_eq!(retained.store.batches, 1);
    assert_eq!(retained.active_timers, 1);
    assert_eq!(retained.completion_bindings, 1);
    host.inject_terminal_interpretation_fault();

    let error = host
        .execution_unavailable(Moment::from_tick(1))
        .err()
        .unwrap_or_else(|| panic!("damaged exact cleanup must remain reportable"));
    assert!(
        error
            .to_string()
            .contains("forced terminal producer interpretation failure")
    );
    assert!(host.terminal_resources_empty());
    wait_until(|| waker_called.load(Ordering::Acquire));
    assert!(
        released_before_wake.load(Ordering::Acquire),
        "raw payload ownership must drop before fallback wakes application code"
    );

    assert_failure(observer.wait(), ProducerDeliveryStatus::PossiblySent);
    assert_eq!(host.unsettled_completions(), 0);
}

#[test]
fn planning_failure_replaces_the_live_core_before_fallback_publication() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    assert!(host.stats().core_retained_bytes.get() > 0);
    assert_eq!(host.stats().core_completion_slots, 1);
    host.inject_terminal_planning_fault();

    let error = host
        .execution_unavailable(Moment::from_tick(1))
        .err()
        .unwrap_or_else(|| panic!("planning failure must remain reportable"));
    assert!(
        error
            .to_string()
            .contains("forced terminal producer planning failure")
    );
    let drained = host.stats();
    assert_eq!(drained.core_retained_bytes.get(), 0);
    assert_eq!(drained.core_completion_slots, 0);
    assert!(host.terminal_resources_empty());
    assert_failure(
        admitted.into_delivery_observer().wait(),
        ProducerDeliveryStatus::PossiblySent,
    );
}

#[test]
fn fallback_failure_retains_primary_and_settlement_diagnostics() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let recovery = host
        .recover_notifier()
        .unwrap_or_else(|error| panic!("notification recovery should remain owned: {error}"));
    host.inject_terminal_interpretation_fault();

    let error = host
        .execution_unavailable(Moment::from_tick(1))
        .err()
        .unwrap_or_else(|| panic!("disconnected fallback must report both failures"));
    let message = error.to_string();
    assert!(message.contains("forced terminal producer interpretation failure"));
    assert!(message.contains("conservative fallback also failed"));
    assert!(message.contains("completion notifier has stopped"));
    assert_eq!(host.stats().store.bytes, 0);
    assert_eq!(host.stats().core_retained_bytes.get(), 0);
    assert_eq!(host.stats().core_completion_slots, 0);
    assert_eq!(host.unsettled_completions(), 1);
    drop(admitted);
    let shutdown = recovery.notifications;
    assert_eq!(
        shutdown.finish_notification_cleanup(),
        super::pending::PendingNotificationShutdownFailures::default()
    );
}

#[test]
fn poisoned_recovery_retries_exact_fifo_then_settles_distinct_reserved_slots() {
    let mut host = start(valid_limits());
    let first = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let second = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("payments"),
    );
    host.inject_terminal_publish_fault(CompletionRegistryError::NotificationBackpressure);
    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    let unknown = OperationId::from_raw(99);
    let fallback = ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
        DeliveryStatus::NotSent,
    ));
    assert_eq!(
        host.interpret_effect_owned(
            Moment::from_tick(6),
            ProducerEffect::Complete {
                operation_id: unknown,
                completion: fallback,
            },
        )
        .map_err(|failure| failure.error()),
        Err(ProducerHostInvariantError::Binding(
            OperationBindingError::UnknownOperation
        ))
    );
    assert_eq!(
        host.terminal_poison().map(RejectedTerminal::operation_id),
        Some(unknown)
    );

    let error = host
        .execution_unavailable(Moment::from_tick(6))
        .err()
        .unwrap_or_else(|| panic!("the first poison must remain reported"));
    assert!(error.to_string().contains("owns no completion binding"));
    assert_eq!(host.stats().terminal_backlog, 0);
    assert!(host.terminal_resources_empty());
    assert_eq!(
        host.poison_reason(),
        Some(ProducerHostInvariantError::Binding(
            OperationBindingError::UnknownOperation
        ))
    );
    let Err(ProducerDeliveryError::Failed(deadline)) = first.into_delivery_observer().wait() else {
        panic!("exact backlogged deadline terminal must publish first")
    };
    assert_eq!(
        deadline.kind(),
        ProducerDeliveryFailureKind::DeadlineElapsed
    );
    assert_eq!(deadline.delivery_status(), ProducerDeliveryStatus::NotSent);
    assert_failure(
        second.into_delivery_observer().wait(),
        ProducerDeliveryStatus::PossiblySent,
    );
}

fn assert_failure(
    result: Result<crate::ProducerRecordMetadata, ProducerDeliveryError>,
    status: ProducerDeliveryStatus,
) {
    let Err(ProducerDeliveryError::Failed(failure)) = result else {
        panic!("execution stop must publish a terminal failure")
    };
    assert_eq!(
        failure.kind(),
        ProducerDeliveryFailureKind::ExecutionUnavailable
    );
    assert_eq!(failure.delivery_status(), status);
}

fn probed_record(dropped: Arc<AtomicBool>) -> super::ProducerRecord {
    super::ProducerRecord::new(
        Arc::from("orders"),
        PartitionIndex::from_raw(0),
        10,
        None,
        Some(Bytes::from_owner(DropOwner {
            bytes: [b'x'],
            dropped,
        })),
    )
}

struct DropOwner {
    bytes: [u8; 1],
    dropped: Arc<AtomicBool>,
}

impl AsRef<[u8]> for DropOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for DropOwner {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

struct ReleaseWitness {
    payload_dropped: Arc<AtomicBool>,
    waker_called: Arc<AtomicBool>,
    released_before_wake: Arc<AtomicBool>,
}

impl Wake for ReleaseWitness {
    fn wake(self: Arc<Self>) {
        let dropped = self.payload_dropped.load(Ordering::Acquire);
        self.released_before_wake.store(dropped, Ordering::Release);
        self.waker_called.store(true, Ordering::Release);
    }
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(Instant::now() < deadline, "notifier should run the waker");
        thread::yield_now();
    }
}
