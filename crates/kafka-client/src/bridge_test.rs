//! Scenarios for exact record semantics across the private engine bridge.

use bytes::Bytes;

use crate::bridge::producer::{into_engine_record, restore_rejected_record};
use crate::record::{Header, Record, RecordParts};

#[test]
fn facade_record_moves_all_nullable_and_ordered_semantics_into_engine() {
    let last_header = Bytes::from_static(b"last");
    let original = Record::from_parts(RecordParts {
        topic: "orders".to_owned(),
        partition: None,
        timestamp_milliseconds: None,
        key: Some(Bytes::new()),
        value: None,
        headers: vec![
            Header::from_parts("trace".to_owned(), None),
            Header::from_parts("trace".to_owned(), Some(Bytes::new())),
            Header::from_parts("trace".to_owned(), Some(last_header.clone())),
        ],
    });
    let expected = original.clone();

    let engine = into_engine_record(original);

    assert_eq!(engine.topic(), "orders");
    assert_eq!(engine.explicit_partition(), None);
    assert_eq!(engine.timestamp(), None);
    assert_eq!(engine.key_bytes(), Some(&Bytes::new()));
    assert_eq!(engine.value_bytes(), None);
    assert_eq!(engine.headers().len(), 3);
    assert_eq!(engine.headers()[0].name(), "trace");
    assert_eq!(engine.headers()[0].value(), None);
    assert_eq!(engine.headers()[1].value(), Some(&Bytes::new()));
    assert_eq!(engine.headers()[2].value(), Some(&last_header));
    assert_eq!(
        engine.headers()[2].value().map(|bytes| bytes.as_ptr()),
        Some(last_header.as_ptr())
    );

    let restored = restore_rejected_record(engine);
    assert_eq!(restored, expected);
    assert_eq!(
        restored.headers()[2].value().map(|bytes| bytes.as_ptr()),
        Some(last_header.as_ptr())
    );
}

#[test]
fn rejected_engine_record_restores_values_without_collapsing_null_or_empty() {
    let original = Record::from_parts(RecordParts {
        topic: "metrics".to_owned(),
        partition: Some(0),
        timestamp_milliseconds: Some(i64::MIN),
        key: None,
        value: Some(Bytes::new()),
        headers: vec![
            Header::from_parts("tag".to_owned(), Some(Bytes::new())),
            Header::from_parts("tag".to_owned(), None),
            Header::from_parts("tag".to_owned(), Some(Bytes::from_static(b"last"))),
        ],
    });
    let expected = original.clone();
    let engine = into_engine_record(original);

    let restored = restore_rejected_record(engine);

    assert_eq!(restored, expected);
    assert_eq!(restored.topic(), "metrics");
    assert_eq!(restored.explicit_partition(), Some(0));
    assert_eq!(restored.timestamp(), Some(i64::MIN));
    assert_eq!(restored.key_bytes(), None);
    assert_eq!(restored.value_bytes(), Some(&Bytes::new()));
    assert_eq!(restored.headers().len(), 3);
    assert_eq!(restored.headers()[0].name(), "tag");
    assert_eq!(restored.headers()[0].value(), Some(&Bytes::new()));
    assert_eq!(restored.headers()[1].value(), None);
    assert_eq!(
        restored.headers()[2].value(),
        Some(&Bytes::from_static(b"last"))
    );

    let round_trip = into_engine_record(restored);
    assert_eq!(round_trip.explicit_partition(), Some(0));
    assert_eq!(round_trip.timestamp(), Some(i64::MIN));
    assert_eq!(round_trip.key_bytes(), None);
    assert_eq!(round_trip.value_bytes(), Some(&Bytes::new()));
    assert_eq!(round_trip.headers()[0].value(), Some(&Bytes::new()));
    assert_eq!(round_trip.headers()[1].value(), None);
    assert_eq!(
        round_trip.headers()[2].value(),
        Some(&Bytes::from_static(b"last"))
    );
}

#[test]
fn nonempty_payload_storage_is_reused_across_rejection_round_trip() {
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

    let engine = into_engine_record(original);
    assert_eq!(
        engine.key_bytes().map(|bytes| bytes.as_ptr()),
        Some(key.as_ptr())
    );
    assert_eq!(
        engine.value_bytes().map(|bytes| bytes.as_ptr()),
        Some(value.as_ptr())
    );

    let restored = restore_rejected_record(engine);
    assert_eq!(
        restored.key_bytes().map(|bytes| bytes.as_ptr()),
        Some(key.as_ptr())
    );
    assert_eq!(
        restored.value_bytes().map(|bytes| bytes.as_ptr()),
        Some(value.as_ptr())
    );
}
