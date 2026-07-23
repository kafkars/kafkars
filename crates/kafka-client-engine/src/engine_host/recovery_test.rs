//! Terminal recovery scenarios for accepted work and retained handles.

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
use kafka_client_core::ByteCount;

use crate::{
    Engine, EngineConfig, ProducerDeliveryError, ProducerDeliveryFailureKind,
    ProducerDeliveryStatus, ProducerHandle, ProducerRecord, ProducerSendOptions,
    ProducerTrySendAccepted, ProducerTrySendErrorKind,
};

#[test]
fn failed_runner_settles_accepted_work_and_closes_retained_handles() {
    let engine = Engine::start(
        EngineConfig::new(vec!["192.0.2.1:9092".to_owned()])
            .with_delivery_timeout(Duration::from_secs(1)),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"));
    engine.pause_after_produce_admission();
    let producer = engine.producer();
    let accepted = admit(&producer, record(), Duration::from_secs(1));
    wait_until(|| engine.host_snapshot().produce_admission_paused);

    engine.force_host_failure();
    assert!(engine.shutdown().is_err());

    let Err(ProducerDeliveryError::Failed(failure)) = accepted.into_observer().wait() else {
        panic!("host failure must settle every accepted observer")
    };
    assert_eq!(
        failure.kind(),
        ProducerDeliveryFailureKind::ExecutionUnavailable
    );
    assert_eq!(
        failure.delivery_status(),
        ProducerDeliveryStatus::PossiblySent
    );

    let error = producer
        .try_send(record(), ProducerSendOptions::new(Duration::from_secs(1)))
        .err()
        .unwrap_or_else(|| panic!("dead host must reject retained handles"));
    assert!(matches!(
        error.kind(),
        ProducerTrySendErrorKind::Closed | ProducerTrySendErrorKind::HostPoisoned
    ));
    let restored = error.into_record();
    assert_eq!(restored.topic(), "orders");
}

#[test]
fn damaged_interpretation_releases_driver_and_bytes_before_retained_failure_report() {
    let timeout = Duration::from_secs(30);
    let engine = Engine::start(
        EngineConfig::new(vec!["192.0.2.1:9092".to_owned()]).with_delivery_timeout(timeout),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"));
    engine.pause_after_produce_admission();
    let producer = engine.producer();
    let payload_dropped = Arc::new(AtomicBool::new(false));
    let accepted = admit(
        &producer,
        record_with_probe(Arc::clone(&payload_dropped)),
        timeout,
    );
    let mut observer = accepted.into_observer();
    let waker_called = Arc::new(AtomicBool::new(false));
    let released_before_wake = Arc::new(AtomicBool::new(false));
    let waker = Waker::from(Arc::new(ReleaseWitness {
        engine: engine.clone(),
        payload_dropped,
        waker_called: Arc::clone(&waker_called),
        released_before_wake: Arc::clone(&released_before_wake),
    }));
    assert_eq!(
        Pin::new(&mut observer).poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    );
    wait_until(|| engine.host_snapshot().produce_admission_paused);
    let retained = producer.shard_stats();
    assert_eq!(retained.host.store.records, 1);
    assert!(retained.host.store.bytes > 0);
    assert_eq!(retained.host.store.batches, 1);
    assert_eq!(retained.host.store.topics, 1);
    assert_eq!(retained.host.prepared_bytes, 0);
    assert_eq!(retained.host.submission_deadlines, 0);
    assert_eq!(retained.host.completion_bindings, 1);
    producer.inject_terminal_interpretation_fault();
    engine.force_host_failure();
    let error = engine
        .shutdown()
        .err()
        .unwrap_or_else(|| panic!("damaged exact cleanup must remain in the retained report"));
    assert!(
        error
            .to_string()
            .contains("forced terminal producer interpretation failure")
    );

    let drained = producer.shard_stats();
    assert_eq!(drained.host.store.records, 0);
    assert_eq!(drained.host.store.bytes, 0);
    assert_eq!(drained.host.store.batches, 0);
    assert_eq!(drained.host.store.topics, 0);
    assert_eq!(drained.host.active_timers, 0);
    assert_eq!(drained.host.prepared_batches, 0);
    assert_eq!(drained.host.prepared_bytes, 0);
    assert_eq!(drained.host.submission_deadlines, 0);
    assert_eq!(drained.host.completion_bindings, 0);
    assert_eq!(drained.host.pending_effects, 0);
    assert_eq!(drained.host.core_retained_bytes, ByteCount::new(0));
    assert_eq!(drained.host.core_completion_slots, 0);
    wait_until(|| waker_called.load(Ordering::Acquire));
    assert!(released_before_wake.load(Ordering::Acquire));

    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("fallback must preserve terminal observer publication")
    };
    assert_eq!(
        failure.kind(),
        ProducerDeliveryFailureKind::ExecutionUnavailable
    );
    assert_eq!(
        failure.delivery_status(),
        ProducerDeliveryStatus::PossiblySent
    );
}

fn record() -> ProducerRecord {
    ProducerRecord::to("orders")
        .partition(0)
        .key(Bytes::from_static(b"key"))
        .value(Bytes::from_static(b"value"))
}

fn record_with_probe(dropped: Arc<AtomicBool>) -> ProducerRecord {
    ProducerRecord::to("orders")
        .partition(0)
        .key(Bytes::from_static(b"key"))
        .value(Bytes::from_owner(DropOwner {
            bytes: *b"value",
            dropped,
        }))
}

fn admit(
    producer: &ProducerHandle,
    mut record: ProducerRecord,
    timeout: Duration,
) -> ProducerTrySendAccepted {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match producer.try_send(record, ProducerSendOptions::new(timeout)) {
            Ok(accepted) => return accepted,
            Err(error) if error.kind() == ProducerTrySendErrorKind::Contended => {
                assert!(
                    Instant::now() < deadline,
                    "healthy producer contention must eventually admit the record"
                );
                record = error.into_record();
                thread::yield_now();
            }
            Err(error) => panic!("pre-failure admission should succeed: {error}"),
        }
    }
}

struct DropOwner {
    bytes: [u8; 5],
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
    engine: Engine,
    payload_dropped: Arc<AtomicBool>,
    waker_called: Arc<AtomicBool>,
    released_before_wake: Arc<AtomicBool>,
}

impl Wake for ReleaseWitness {
    fn wake(self: Arc<Self>) {
        self.released_before_wake.store(
            self.payload_dropped.load(Ordering::Acquire)
                && self.engine.host_snapshot().recovery_driver_released,
            Ordering::Release,
        );
        self.waker_called.store(true, Ordering::Release);
    }
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "producer stage should become visible before its deadline"
        );
        thread::yield_now();
    }
}
