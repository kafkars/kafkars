//! Generated Kafka transaction-control schema composition scenarios.

use bytes::{Bytes, BytesMut};
use kafka_wire::{ControlRecordTypeSchema, EndTxnMarker};
use kafka_wire_core::{ApiVersion, Encoder, KafkaEncode};

use super::{
    FetchBatch, FetchHeader, FetchProducerIdentity, FetchRecord, FetchTimestampType,
    control_record::{FetchControlRecordFailure, FetchControlRecordKind, decode_control_record},
};

#[test]
fn generated_schemas_decode_abort_and_commit_markers_exactly() {
    for (control_type, expected) in [
        (0, FetchControlRecordKind::Abort),
        (1, FetchControlRecordKind::Commit),
        (7, FetchControlRecordKind::Other(7)),
    ] {
        let batch = marker_batch(control_type, 0);
        assert_eq!(decode_control_record(&batch), Ok(expected));
    }
}

#[test]
fn schema_versions_and_trailing_bytes_are_strict() {
    let mut unsupported = marker_batch(0, 0);
    unsupported.records[0].key = Some(Bytes::from_static(&[0, 1, 0, 0]));
    assert!(matches!(
        decode_control_record(&unsupported),
        Err(FetchControlRecordFailure::Key(
            kafka_wire_core::DecodeError::UnsupportedVersion { .. }
        ))
    ));

    let mut trailing = marker_batch(0, 0);
    let mut value = marker_value(0);
    value.extend_from_slice(&[0]);
    trailing.records[0].value = Some(value.freeze());
    assert_eq!(
        decode_control_record(&trailing),
        Err(FetchControlRecordFailure::Value(
            kafka_wire_core::DecodeError::TrailingBytes { remaining: 1 }
        ))
    );
}

#[test]
fn marker_shape_requires_one_nonnull_key_and_value() {
    let mut missing_key = marker_batch(0, 0);
    missing_key.records[0].key = None;
    assert_eq!(
        decode_control_record(&missing_key),
        Err(FetchControlRecordFailure::MissingKey)
    );

    let mut missing_value = marker_batch(0, 0);
    missing_value.records[0].value = None;
    assert_eq!(
        decode_control_record(&missing_value),
        Err(FetchControlRecordFailure::MissingValue)
    );

    let mut two = marker_batch(0, 0);
    two.records.push(record(0, 0));
    assert_eq!(
        decode_control_record(&two),
        Err(FetchControlRecordFailure::RecordCount { actual: 2 })
    );
}

fn marker_batch(control_type: i16, version: i16) -> FetchBatch {
    FetchBatch {
        base_offset: 4,
        last_offset: 4,
        next_offset: 5,
        partition_leader_epoch: None,
        timestamp_type: FetchTimestampType::Create,
        max_timestamp: Some(1),
        producer: Some(FetchProducerIdentity {
            producer_id: 8,
            producer_epoch: 1,
            base_sequence: -1,
        }),
        is_transactional: true,
        is_control: true,
        delete_horizon_ms: None,
        records: vec![record(control_type, version)],
    }
}

fn record(control_type: i16, version: i16) -> FetchRecord {
    FetchRecord {
        attributes: 0,
        offset: 4,
        timestamp: Some(1),
        key: Some(marker_key(control_type, version).freeze()),
        value: Some(marker_value(version).freeze()),
        headers: Vec::<FetchHeader>::new(),
    }
}

fn marker_key(control_type: i16, version: i16) -> BytesMut {
    let mut bytes = BytesMut::new();
    let mut encoder = Encoder::new(&mut bytes);
    encoder
        .write_i16(version)
        .unwrap_or_else(|error| panic!("control key version: {error}"));
    let mut key = ControlRecordTypeSchema::default();
    key.type_ = control_type;
    key.encode(&mut encoder, ApiVersion::new(version))
        .unwrap_or_else(|error| panic!("generated control key: {error}"));
    bytes
}

fn marker_value(version: i16) -> BytesMut {
    let mut bytes = BytesMut::new();
    let mut encoder = Encoder::new(&mut bytes);
    encoder
        .write_i16(version)
        .unwrap_or_else(|error| panic!("marker value version: {error}"));
    let mut marker = EndTxnMarker::default();
    marker.coordinator_epoch = 3;
    marker
        .encode(&mut encoder, ApiVersion::new(version))
        .unwrap_or_else(|error| panic!("generated marker value: {error}"));
    bytes
}
