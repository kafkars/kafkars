//! Call-boundary ordering and exact facade-owner producer rejection evidence.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_engine::{Engine, EngineConfig};

use super::ProducerEngine;
use crate::{
    DeliveryStatus, ErrorKind,
    record::{Header, Record, RecordParts},
};

#[test]
fn capture_failure_precedes_conversion_and_returns_exact_facade_record() {
    let engine = start_engine();
    let producer = ProducerEngine::new(engine.producer(), Duration::MAX);
    let retained = Bytes::from(vec![1, 2, 3, 4]);
    let record = Record::from_parts(RecordParts {
        topic: String::new().into(),
        partition: None,
        timestamp_milliseconds: None,
        key: Some(Bytes::new()),
        value: None,
        headers: vec![
            Header::from_parts("trace".to_owned(), None),
            Header::from_parts("trace".to_owned(), Some(Bytes::new())),
            Header::from_parts("trace".to_owned(), Some(retained.clone())),
        ],
    });
    let header_allocation = record.headers().as_ptr();

    let Err(rejection) = producer.try_send(record) else {
        panic!("unrepresentable boundary must reject before record conversion")
    };
    let (returned, error) = rejection.into_parts();

    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    assert_eq!(returned.headers().as_ptr(), header_allocation);
    assert_eq!(returned.topic(), "");
    assert_eq!(returned.key_bytes(), Some(&Bytes::new()));
    assert_eq!(returned.value_bytes(), None);
    assert_eq!(returned.headers()[0].value(), None);
    assert_eq!(returned.headers()[1].value(), Some(&Bytes::new()));
    assert_eq!(
        returned.headers()[2].value().map(|bytes| bytes.as_ptr()),
        Some(retained.as_ptr())
    );
}

#[test]
fn engine_pre_admission_rejection_returns_the_exact_original_record() {
    let engine = start_engine();
    let producer = ProducerEngine::new(engine.producer(), Duration::from_secs(1));
    let topic: Arc<str> = Arc::from("orders");
    let value = Bytes::from(vec![4, 5, 6]);
    let mut record = Record::to(Arc::clone(&topic))
        .partition(-1)
        .value(value.clone())
        .header("trace", "value");
    let header_allocation = record.headers().as_ptr();
    let deadline = Instant::now() + Duration::from_secs(1);

    let (returned, error) = loop {
        match producer.try_send(record) {
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                if error.kind() != ErrorKind::Backpressure {
                    break (returned, error);
                }
                assert!(Instant::now() < deadline, "producer remained contended");
                record = returned;
                std::hint::spin_loop();
            }
            Ok(_delivery) => panic!("negative partition must reject before admission"),
        }
    };

    assert_eq!(error.kind(), ErrorKind::InvalidRecord);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    assert!(Arc::ptr_eq(returned.topic_owner(), &topic));
    assert_eq!(returned.headers().as_ptr(), header_allocation);
    assert_eq!(
        returned.value_bytes().map(|bytes| bytes.as_ptr()),
        Some(value.as_ptr())
    );
}

fn start_engine() -> Engine {
    let result = Engine::start(EngineConfig::new(vec!["127.0.0.1:1".to_owned()]));
    let Ok(engine) = result else {
        panic!("valid local engine configuration should start")
    };
    engine
}
