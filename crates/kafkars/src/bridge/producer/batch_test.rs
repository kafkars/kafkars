//! Private producer batch aggregate ready-state scenarios.

use std::time::Duration;

use kafka_client_engine::{Engine, EngineConfig};

use super::batch::ProducerBatch;
use crate::{ErrorKind, Record, silent_broker_test::SilentBroker};

use super::handle::ProducerEngine;

#[test]
fn empty_aggregate_retains_an_empty_terminal_result() {
    let result = ProducerBatch::new(Vec::new(), None).wait();

    assert!(result.deliveries().is_empty());
    assert!(result.rejection().is_none());
}

#[test]
fn blocking_wait_reuses_a_result_stored_by_an_earlier_partial_poll() {
    let broker = SilentBroker::start();
    let engine = Engine::start(EngineConfig::new(vec![broker.endpoint()]))
        .unwrap_or_else(|error| panic!("valid local engine should start: {error}"));
    let second = accepted_delivery(&engine, Duration::from_millis(5), "second");
    let batch = ProducerBatch::from_partially_polled_test_state(
        Err(crate::KafkaError::new(
            ErrorKind::Timeout,
            "stored first result",
        )),
        second,
    );
    broker.wait_negotiated();
    let result = batch.wait();

    assert_eq!(result.deliveries().len(), 2);
    assert!(matches!(
        result.deliveries().first(),
        Some(Err(error)) if error.kind() == ErrorKind::Timeout
    ));
}

fn accepted_delivery(engine: &Engine, timeout: Duration, value: &str) -> super::ProducerDelivery {
    let producer = ProducerEngine::new(engine.producer(), timeout);
    let mut record = Record::to("orders").partition(0).value(value.to_owned());
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        match producer.try_send(record) {
            Ok(delivery) => return delivery,
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                assert_eq!(error.kind(), ErrorKind::Backpressure);
                assert!(
                    std::time::Instant::now() < deadline,
                    "record should reach admission"
                );
                record = returned;
                std::hint::spin_loop();
            }
        }
    }
}
