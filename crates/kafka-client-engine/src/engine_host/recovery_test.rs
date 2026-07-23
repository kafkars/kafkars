//! Terminal recovery scenarios for accepted work and retained handles.

use std::time::Duration;

use bytes::Bytes;

use crate::{
    Engine, EngineConfig, ProducerDeliveryError, ProducerDeliveryFailureKind,
    ProducerDeliveryStatus, ProducerRecord, ProducerSendOptions, ProducerTrySendErrorKind,
};

#[test]
fn failed_runner_settles_accepted_work_and_closes_retained_handles() {
    let engine = Engine::start(
        EngineConfig::new(vec!["192.0.2.1:9092".to_owned()])
            .with_delivery_timeout(Duration::from_secs(1)),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"));
    let producer = engine.producer();
    let accepted = producer
        .try_send(record(), ProducerSendOptions::new(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("pre-failure admission should succeed: {error}"));

    engine.force_host_failure();
    assert!(engine.shutdown().is_err());

    let Err(ProducerDeliveryError::Failed(failure)) = accepted.into_observer().wait() else {
        panic!("host failure must settle every accepted observer")
    };
    assert_eq!(
        failure.kind(),
        ProducerDeliveryFailureKind::ExecutionUnavailable
    );
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);

    let error = producer
        .try_send(record(), ProducerSendOptions::new(Duration::from_secs(1)))
        .err()
        .unwrap_or_else(|| panic!("dead host must reject retained handles"));
    assert!(matches!(
        error.kind(),
        ProducerTrySendErrorKind::Closed | ProducerTrySendErrorKind::HostPoisoned
    ));
    let restored = error
        .into_record()
        .unwrap_or_else(|| panic!("failed host must preserve caller ownership"));
    assert_eq!(restored.topic(), "orders");
}

fn record() -> ProducerRecord {
    ProducerRecord::to("orders")
        .partition(0)
        .key(Bytes::from_static(b"key"))
        .value(Bytes::from_static(b"value"))
}
