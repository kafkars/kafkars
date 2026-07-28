//! Scenarios for call-boundary ordering and exact facade record rejection.

use std::time::Duration;

use bytes::Bytes;
use kafka_client_engine::{Engine, EngineConfig};

use super::{ProducerEngine, into_engine_record, restore_rejected_record};
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
        topic: String::new(),
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

    let Err(rejection) = producer.try_send(record) else {
        panic!("unrepresentable boundary must reject before record conversion")
    };
    let (returned, error) = rejection.into_parts();

    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
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
fn record_bridge_preserves_nullable_and_ordered_semantics() {
    let retained = Bytes::from_static(b"last");
    let original = Record::from_parts(RecordParts {
        topic: "orders".to_owned(),
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
    let expected = original.clone();

    let engine = into_engine_record(original);

    assert_eq!(engine.topic(), "orders");
    assert_eq!(engine.explicit_partition(), None);
    assert_eq!(engine.timestamp(), None);
    assert_eq!(engine.key_bytes(), Some(&Bytes::new()));
    assert_eq!(engine.value_bytes(), None);
    assert_eq!(engine.headers()[0].value(), None);
    assert_eq!(engine.headers()[1].value(), Some(&Bytes::new()));
    assert_eq!(
        engine.headers()[2].value().map(|bytes| bytes.as_ptr()),
        Some(retained.as_ptr())
    );

    let restored = restore_rejected_record(engine);
    assert_eq!(restored, expected);
    assert_eq!(
        restored.headers()[2].value().map(|bytes| bytes.as_ptr()),
        Some(retained.as_ptr())
    );
}

#[test]
fn rejected_record_reuses_nonempty_payload_storage() {
    let key = Bytes::from(vec![1, 2, 3]);
    let value = Bytes::from(vec![4, 5, 6]);
    let original = Record::from_parts(RecordParts {
        topic: "owned".to_owned(),
        partition: Some(2),
        timestamp_milliseconds: Some(9),
        key: Some(key.clone()),
        value: Some(value.clone()),
        headers: Vec::new(),
    });

    let restored = restore_rejected_record(into_engine_record(original));

    assert_eq!(
        restored.key_bytes().map(|bytes| bytes.as_ptr()),
        Some(key.as_ptr())
    );
    assert_eq!(
        restored.value_bytes().map(|bytes| bytes.as_ptr()),
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
