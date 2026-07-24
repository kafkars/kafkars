//! Integrated tracked Produce execution and reactor-parking scenarios.

use std::{thread, time::Duration};

use bytes::Bytes;

use crate::{
    Engine, EngineConfig, ProducerDeliveryError, ProducerDeliveryFailureKind,
    ProducerDeliveryStatus, ProducerHandle, ProducerRecord, ProducerSendOptions,
    silent_broker_test::SilentBroker,
};

#[test]
fn tracked_produce_failure_parks_without_spinning() {
    let timeout = Duration::from_millis(200);
    let broker = SilentBroker::start();
    let engine = start(timeout, broker.endpoint());
    let accepted = admit(&engine.producer(), timeout);
    assert!(accepted.fault().is_none());

    thread::sleep(Duration::from_millis(40));
    let before = engine.host_snapshot();
    thread::sleep(Duration::from_millis(60));
    let after = engine.host_snapshot();

    // Connection establishment may contribute a few legitimate driver-local
    // turns. An immediate host spin would contribute orders of magnitude more.
    assert!(
        after.producer_turns.saturating_sub(before.producer_turns) <= 4,
        "tracked Produce failure must not accumulate producer turns: {before:?} -> {after:?}"
    );
    assert!(
        after.driver_turns.saturating_sub(before.driver_turns) <= 4,
        "tracked Produce failure must not accumulate driver turns: {before:?} -> {after:?}"
    );
    let Err(ProducerDeliveryError::Failed(failure)) = accepted.into_observer().wait() else {
        panic!("silent broker work must fail at its public deadline")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
    engine
        .shutdown()
        .unwrap_or_else(|error| panic!("clean engine shutdown should join: {error}"));
}

fn start(timeout: Duration, endpoint: String) -> Engine {
    Engine::start(
        EngineConfig::new(vec![endpoint])
            .with_delivery_timeout(timeout)
            .with_producer_retry(0, Duration::ZERO),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"))
}

fn admit(producer: &ProducerHandle, timeout: Duration) -> crate::ProducerTrySendAccepted {
    let mut pending = ProducerRecord::to("orders")
        .partition(0)
        .key(Bytes::from_static(b"key"))
        .value(Bytes::from_static(b"value"));
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
