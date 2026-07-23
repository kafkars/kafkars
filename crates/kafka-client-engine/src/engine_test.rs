//! Integrated engine-host lifecycle, parking, and failure scenarios.

use std::{thread, time::Duration};

use bytes::Bytes;

use crate::{
    Engine, EngineConfig, EngineStartErrorKind, ProducerDeliveryError, ProducerDeliveryFailureKind,
    ProducerDeliveryStatus, ProducerHandle, ProducerRecord, ProducerSendOptions,
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
fn prepared_produce_parks_until_the_original_deadline_without_spinning() {
    let timeout = Duration::from_millis(200);
    let engine = start(timeout);
    let producer = engine.producer();
    let accepted = producer
        .try_send(record(), ProducerSendOptions::new(timeout))
        .unwrap_or_else(|error| panic!("record admission should succeed: {error}"));
    assert!(accepted.fault().is_none());

    thread::sleep(Duration::from_millis(40));
    let before = engine.host_snapshot();
    thread::sleep(Duration::from_millis(60));
    let after = engine.host_snapshot();

    // Connection establishment may contribute a few legitimate driver-local
    // turns. An immediate host spin would contribute orders of magnitude more.
    assert!(
        after.producer_turns.saturating_sub(before.producer_turns) <= 4,
        "parked pre-driver work must not accumulate producer turns: {before:?} -> {after:?}"
    );
    assert!(
        after.driver_turns.saturating_sub(before.driver_turns) <= 4,
        "parked pre-driver work must not accumulate driver turns: {before:?} -> {after:?}"
    );
    assert_deadline_not_sent(accepted.into_observer().wait());
    engine
        .shutdown()
        .unwrap_or_else(|error| panic!("clean engine shutdown should join: {error}"));
}

#[test]
fn producer_handle_retains_the_host_after_parent_engine_drop() {
    let timeout = Duration::from_millis(30);
    let engine = start(timeout);
    let producer = engine.producer();
    drop(engine);

    let accepted = producer
        .try_send(record(), ProducerSendOptions::new(timeout))
        .unwrap_or_else(|error| panic!("retained child handle must keep host live: {error}"));

    assert_deadline_not_sent(accepted.into_observer().wait());
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

fn assert_deadline_not_sent(result: Result<crate::ProducerRecordMetadata, ProducerDeliveryError>) {
    let Err(ProducerDeliveryError::Failed(failure)) = result else {
        panic!("parked pre-driver work must fail at its deadline")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}
