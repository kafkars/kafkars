//! Fetch normalization scenarios across DTO, record-codec, and engine boundaries.

use bytes::{Bytes, BytesMut};
use kafka_wire::{
    FetchResponse as WireFetchResponse,
    fetch_response::{AbortedTransaction, FetchableTopicResponse, NodeEndpoint, PartitionData},
};
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordEncodeLimits, RecordHeader, TimestampType,
};

use super::{FetchDecodeFailure, FetchDecodeLimits, FetchTimestampType, normalize_fetch_response};

#[test]
fn response_order_and_semantic_broker_facts_are_preserved() {
    let mut first = partition(3, Some(record_bytes(Compression::None)));
    first.error_code = -321;
    first.high_watermark = 90;
    first.last_stable_offset = 80;
    first.log_start_offset = 4;
    first.current_leader.leader_id = 7;
    first.current_leader.leader_epoch = 11;
    first.preferred_read_replica = 9;
    first.diverging_epoch.epoch = 2;
    first.diverging_epoch.end_offset = 75;
    first.aborted_transactions = Some(vec![aborted(44, 12), aborted(45, 18)]);
    let second = partition(1, None);
    let mut response = response(vec![
        topic("zeta", vec![first, second]),
        topic("alpha", vec![partition(0, None)]),
    ]);
    response.throttle_time_ms = 5;
    response.error_code = -123;
    response.session_id = 17;
    response.node_endpoints = vec![endpoint(7, "leader.local", 9092, Some("rack-a"))];

    let normalized = normalize_fetch_response(response, FetchDecodeLimits::default())
        .unwrap_or_else(|error| panic!("valid fetch response: {error:?}"));

    assert_eq!(normalized.throttle_time_ms, 5);
    assert_eq!(normalized.error_code, -123);
    assert_eq!(normalized.session_id, 17);
    assert_eq!(normalized.topics[0].name, "zeta");
    assert_eq!(normalized.topics[0].topic_id, [0; 16]);
    assert_eq!(normalized.topics[0].partitions[0].index, 3);
    assert_eq!(normalized.topics[0].partitions[0].error_code, -321);
    assert_eq!(normalized.topics[0].partitions[0].high_watermark, Some(90));
    assert_eq!(normalized.topics[0].partitions[1].index, 1);
    assert_eq!(normalized.topics[1].name, "alpha");
    assert_eq!(normalized.endpoints[0].host, "leader.local");
    assert_eq!(
        normalized.endpoints[0].rack.as_deref(),
        Some(&b"rack-a"[..])
    );
}

#[test]
fn records_keep_offsets_nullability_and_duplicate_header_order() {
    let response = response(vec![topic(
        "events",
        vec![partition(2, Some(record_bytes(Compression::None)))],
    )]);

    let normalized = normalize_fetch_response(response, FetchDecodeLimits::default())
        .unwrap_or_else(|error| panic!("valid record batch: {error:?}"));
    let batch = &normalized.topics[0].partitions[0].batches[0];
    let records = &batch.records;

    assert_eq!(batch.base_offset, 40);
    assert_eq!(batch.last_offset, 42);
    assert_eq!(batch.timestamp_type, FetchTimestampType::Create);
    assert_eq!(records[0].offset, 40);
    assert_eq!(records[1].offset, 42);
    assert_eq!(records[0].timestamp, Some(1_000));
    assert_eq!(records[1].timestamp, Some(1_005));
    assert!(records[0].key.is_none());
    assert_eq!(records[0].value.as_deref(), Some(&b""[..]));
    assert_eq!(records[1].key.as_deref(), Some(&b""[..]));
    assert!(records[1].value.is_none());
    assert_eq!(records[1].headers.len(), 3);
    assert_eq!(records[1].headers[0].key, "trace");
    assert_eq!(records[1].headers[0].value.as_deref(), Some(&b"first"[..]));
    assert_eq!(records[1].headers[1].key, "trace");
    assert!(records[1].headers[1].value.is_none());
    assert_eq!(records[1].headers[2].key, "trace");
    assert_eq!(records[1].headers[2].value.as_deref(), Some(&b""[..]));
}

#[test]
fn wire_records_remains_the_compression_and_crc_authority() {
    let compressed_response = response(vec![topic(
        "compressed",
        vec![partition(0, Some(record_bytes(Compression::Gzip)))],
    )]);
    let normalized = normalize_fetch_response(compressed_response, FetchDecodeLimits::default())
        .unwrap_or_else(|error| panic!("wire-records should decode gzip: {error:?}"));
    assert_eq!(
        normalized.topics[0].partitions[0].batches[0].records[1].headers[0]
            .value
            .as_deref(),
        Some(&b"first"[..])
    );

    let mut corrupt = BytesMut::from(record_bytes(Compression::None).as_ref());
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0x80;
    let corrupt_response = response(vec![topic(
        "corrupt",
        vec![partition(0, Some(corrupt.freeze()))],
    )]);
    assert!(matches!(
        normalize_fetch_response(corrupt_response, FetchDecodeLimits::default()),
        Err(FetchDecodeFailure::RecordBatch {
            topic: 0,
            partition: 0,
            batch: 0,
            source: kafka_wire_records::RecordError::CorruptBatch { .. },
        })
    ));
}

#[test]
fn response_record_budget_fails_only_normalization() {
    let limits = FetchDecodeLimits {
        max_records: 1,
        ..FetchDecodeLimits::default()
    };
    assert_eq!(
        normalize_fetch_response(
            response(vec![topic(
                "bounded",
                vec![partition(0, Some(record_bytes(Compression::None)))],
            )]),
            limits,
        ),
        Err(FetchDecodeFailure::RecordCount {
            actual: 2,
            limit: 1,
        })
    );
}

pub(super) fn response(topics: Vec<FetchableTopicResponse>) -> WireFetchResponse {
    let mut response = WireFetchResponse::default();
    response.responses = topics;
    response
}

pub(super) fn topic(name: &str, partitions: Vec<PartitionData>) -> FetchableTopicResponse {
    let mut topic = FetchableTopicResponse::default();
    topic.topic = name.into();
    topic.partitions = partitions;
    topic
}

pub(super) fn partition(index: i32, records: Option<Bytes>) -> PartitionData {
    let mut partition = PartitionData::default();
    partition.partition_index = index;
    partition.records = records;
    partition
}

fn endpoint(node_id: i32, host: &str, port: i32, rack: Option<&str>) -> NodeEndpoint {
    let mut endpoint = NodeEndpoint::default();
    endpoint.node_id = node_id;
    endpoint.host = host.into();
    endpoint.port = port;
    endpoint.rack = rack.map(Into::into);
    endpoint
}

fn aborted(producer_id: i64, first_offset: i64) -> AbortedTransaction {
    let mut transaction = AbortedTransaction::default();
    transaction.producer_id = producer_id;
    transaction.first_offset = first_offset;
    transaction
}

pub(super) fn record_bytes(compression: Compression) -> Bytes {
    record_bytes_at(compression, 40)
}

pub(super) fn record_bytes_at(compression: Compression, base_offset: i64) -> Bytes {
    batch_bytes(&RecordBatch {
        base_offset,
        last_offset_delta: 2,
        partition_leader_epoch: 11,
        compression,
        timestamp_type: TimestampType::CreateTime,
        is_transactional: false,
        is_control: false,
        has_delete_horizon: false,
        base_timestamp: 1_000,
        max_timestamp: 1_005,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: vec![
            Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::new()),
                headers: Vec::new(),
            },
            Record {
                attributes: 0,
                timestamp_delta: 5,
                offset_delta: 2,
                key: Some(Bytes::new()),
                value: None,
                headers: vec![
                    header(Some(Bytes::from_static(b"first"))),
                    header(None),
                    header(Some(Bytes::new())),
                ],
            },
        ],
    })
}

pub(super) fn batch_bytes(batch: &RecordBatch) -> Bytes {
    batch
        .encode_to_bytes(RecordEncodeLimits::default())
        .unwrap_or_else(|error| panic!("test record batch encoding: {error}"))
}

fn header(value: Option<Bytes>) -> RecordHeader {
    RecordHeader {
        key: "trace".into(),
        value,
    }
}
