//! Integrated engine-host lifecycle, parking, and failure scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, mpsc::sync_channel},
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;

use crate::{
    Engine, EngineConfig, EngineShutdownError, EngineShutdownErrorKind, EngineStartErrorKind,
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus, ProducerHandle,
    ProducerRecord, ProducerSendOptions,
};

#[test]
fn engine_and_producer_handle_are_shared_native_capabilities() {
    fn require_shared_clone<T: Clone + Send + Sync>() {}

    require_shared_clone::<Engine>();
    require_shared_clone::<ProducerHandle>();
}

#[test]
fn startup_validation_precedes_native_resource_acquisition() {
    let error = Engine::start(EngineConfig::new(Vec::new()))
        .err()
        .unwrap_or_else(|| panic!("empty bootstrap configuration must fail"));

    assert_eq!(error.kind(), EngineStartErrorKind::Configuration);
}

#[test]
fn producer_handle_retains_the_host_after_parent_engine_drop() {
    let timeout = Duration::from_millis(30);
    let engine = start(timeout);
    let producer = engine.producer();
    drop(engine);

    let accepted = admit(&producer, timeout);

    assert_deadline_not_sent(accepted.into_observer().wait());
}

#[test]
fn concurrent_shutdown_callers_observe_one_retained_report() {
    let timeout = Duration::from_millis(80);
    let engine = start(timeout);
    let producer = engine.producer();
    let accepted = admit(&producer, timeout);
    let concurrent = engine.clone();
    let waiter = thread::spawn(move || concurrent.shutdown());
    wait_until(|| engine.host_is_closing());

    wait_for_closed_rejection(&producer, timeout);
    assert!(engine.shutdown().is_ok());
    assert!(waiter.join().is_ok_and(|result| result.is_ok()));
    assert!(engine.host_is_closed());
    assert_execution_not_sent(accepted.into_observer().wait());
}

#[test]
fn shutdown_settles_a_pending_record_then_its_flush_barrier() {
    let timeout = Duration::from_millis(80);
    let engine = start(timeout);
    let producer = engine.producer();
    let accepted = admit(&producer, timeout);
    let flush = admit_flush(&producer);
    let concurrent = engine.clone();
    let waiter = thread::spawn(move || concurrent.shutdown());
    wait_until(|| engine.host_is_closing());

    wait_for_closed_flush_rejection(&producer);
    assert!(waiter.join().is_ok_and(|result| result.is_ok()));
    assert!(engine.host_is_closed());
    assert_execution_not_sent(accepted.into_observer().wait());
    assert_eq!(flush.into_observer().wait(), Ok(()));
}

#[test]
fn notifier_reentrant_shutdown_returns_without_blocking_host_cleanup() {
    let timeout = Duration::from_millis(30);
    let engine = start(timeout);
    let producer = engine.producer();
    let accepted = admit(&producer, timeout);
    let mut observer = accepted.into_observer();
    let (result_sender, result_receiver) = sync_channel(1);
    let waker = Waker::from(Arc::new(ShutdownWake {
        engine: engine.clone(),
        result_sender,
    }));
    let mut context = Context::from_waker(&waker);
    assert_eq!(Pin::new(&mut observer).poll(&mut context), Poll::Pending);

    let result = result_receiver
        .recv_timeout(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("notifier shutdown result should arrive: {error}"));
    let error = result
        .err()
        .unwrap_or_else(|| panic!("notifier shutdown must report deferred observation"));
    assert_eq!(error.kind(), EngineShutdownErrorKind::NotifierThread);
    assert!(engine.shutdown().is_ok());
    assert!(engine.host_is_closed());
    assert_deadline_not_sent(observer.wait());
}

#[test]
fn final_engine_drop_on_notifier_still_reaches_closed() {
    let timeout = Duration::from_millis(30);
    let engine = start(timeout);
    let probe = engine.host_probe();
    let producer = engine.producer();
    let accepted = admit(&producer, timeout);
    let mut observer = accepted.into_observer();
    let (result_sender, result_receiver) = sync_channel(1);
    let waker = Waker::from(Arc::new(ShutdownWake {
        engine,
        result_sender,
    }));
    {
        let mut context = Context::from_waker(&waker);
        assert_eq!(Pin::new(&mut observer).poll(&mut context), Poll::Pending);
    }
    drop(waker);
    drop(producer);

    let result = result_receiver
        .recv_timeout(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("notifier shutdown result should arrive: {error}"));
    assert_eq!(
        result.err().map(|error| error.kind()),
        Some(EngineShutdownErrorKind::NotifierThread)
    );
    assert!(probe.wait_closed(Duration::from_secs(2)));
    assert_deadline_not_sent(observer.wait());
}

fn start(timeout: Duration) -> Engine {
    Engine::start(
        EngineConfig::new(vec!["192.0.2.1:9092".to_owned()]).with_delivery_timeout(timeout),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"))
}

fn record() -> ProducerRecord {
    ProducerRecord::to("orders")
        .partition(0)
        .key(Bytes::from_static(b"key"))
        .value(Bytes::from_static(b"value"))
}

fn admit(producer: &ProducerHandle, timeout: Duration) -> crate::ProducerTrySendAccepted {
    let mut pending = record();
    for _attempt in 0..1_000 {
        match producer.try_send(pending, ProducerSendOptions::new(timeout)) {
            Ok(accepted) => return accepted,
            Err(error) if error.kind() == crate::ProducerTrySendErrorKind::Contended => {
                pending = error.into_record();
                thread::yield_now();
            }
            Err(error) => panic!("record admission should succeed: {error}"),
        }
    }
    panic!("record admission should make progress within the bounded test loop")
}

fn admit_flush(producer: &ProducerHandle) -> crate::ProducerTryFlushAccepted {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match producer.try_flush() {
            Ok(accepted) => return accepted,
            Err(error) if error.kind() == crate::ProducerTryFlushErrorKind::Contended => {
                assert!(
                    Instant::now() < deadline,
                    "healthy producer contention must eventually admit the flush"
                );
                thread::yield_now();
            }
            Err(error) => panic!("flush admission should succeed: {error}"),
        }
    }
}

fn wait_for_closed_flush_rejection(producer: &ProducerHandle) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match producer.try_flush() {
            Err(error) if error.kind() == crate::ProducerTryFlushErrorKind::Contended => {
                assert!(
                    Instant::now() < deadline,
                    "shutdown contention must eventually reveal closed flush admission"
                );
                thread::yield_now();
            }
            Err(error) => {
                assert_eq!(error.kind(), crate::ProducerTryFlushErrorKind::Closed);
                return;
            }
            Ok(_accepted) => panic!("closing admission must not accept another flush"),
        }
    }
}

fn wait_for_closed_rejection(producer: &ProducerHandle, timeout: Duration) {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut pending = record();
    loop {
        match producer.try_send(pending, ProducerSendOptions::new(timeout)) {
            Err(error) if error.kind() == crate::ProducerTrySendErrorKind::Contended => {
                assert!(
                    Instant::now() < deadline,
                    "shutdown contention must eventually reveal closed admission"
                );
                pending = error.into_record();
                thread::yield_now();
            }
            Err(error) => {
                assert_eq!(error.kind(), crate::ProducerTrySendErrorKind::Closed);
                return;
            }
            Ok(_accepted) => panic!("closing admission must not accept another record"),
        }
    }
}

fn assert_deadline_not_sent(result: Result<crate::ProducerRecordMetadata, ProducerDeliveryError>) {
    let Err(ProducerDeliveryError::Failed(failure)) = result else {
        panic!("parked pre-driver work must fail at its deadline")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}

fn assert_execution_not_sent(result: Result<crate::ProducerRecordMetadata, ProducerDeliveryError>) {
    let Err(ProducerDeliveryError::Failed(failure)) = result else {
        panic!("shutdown must settle accepted work")
    };
    assert!(
        matches!(
            failure.kind(),
            ProducerDeliveryFailureKind::DeadlineElapsed | ProducerDeliveryFailureKind::Transport
        ),
        "shutdown may race an authoritative local driver failure with the original deadline"
    );
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}

struct ShutdownWake {
    engine: Engine,
    result_sender: std::sync::mpsc::SyncSender<Result<(), EngineShutdownError>>,
}

impl Wake for ShutdownWake {
    fn wake(self: Arc<Self>) {
        let _sent = self.result_sender.send(self.engine.shutdown());
    }
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "lifecycle condition should become visible"
        );
        thread::yield_now();
    }
}
