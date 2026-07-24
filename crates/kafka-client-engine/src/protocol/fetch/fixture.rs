//! Test-only raw Fetch fixtures normalized before leaving the protocol boundary.

use bytes::{Bytes, BytesMut};
use kafka_wire::{
    FetchResponse as WireFetchResponse,
    fetch_response::{FetchableTopicResponse, PartitionData},
};
use kafka_wire_records::{Compression, Record, RecordBatch, RecordEncodeLimits, TimestampType};

use super::{
    FetchDecodeLimits, FetchOutputReservation, RetainedFetchOutcome,
    normalize_read_uncommitted_fetch_outcome,
};

const SELECTED_VERSION: i16 = 12;

/// Builds and normalizes a successful Fetch response without exposing wire DTOs.
pub(crate) fn retained_success_for_test(
    topic: &str,
    partition: u32,
    requested_offset: i64,
    records: Option<Bytes>,
    reservation: FetchOutputReservation,
) -> RetainedFetchOutcome {
    normalize(
        topic,
        partition,
        requested_offset,
        response(topic, partition, records),
        reservation,
    )
}

/// Builds and normalizes a top-level broker failure without exposing wire DTOs.
pub(crate) fn retained_broker_failure_for_test(
    topic: &str,
    partition: u32,
    requested_offset: i64,
    error_code: i16,
    reservation: FetchOutputReservation,
) -> RetainedFetchOutcome {
    let mut response = WireFetchResponse::default();
    response.error_code = error_code;
    normalize(topic, partition, requested_offset, response, reservation)
}

fn normalize(
    topic: &str,
    partition: u32,
    requested_offset: i64,
    response: WireFetchResponse,
    reservation: FetchOutputReservation,
) -> RetainedFetchOutcome {
    normalize_read_uncommitted_fetch_outcome(
        topic,
        partition,
        requested_offset,
        SELECTED_VERSION,
        response,
        FetchDecodeLimits::default(),
        reservation,
    )
    .unwrap_or_else(|rejected| panic!("normalize Fetch fixture: {:?}", rejected.failure()))
}

fn response(topic: &str, partition: u32, records: Option<Bytes>) -> WireFetchResponse {
    let mut partition_response = PartitionData::default();
    partition_response.partition_index = i32::try_from(partition)
        .unwrap_or_else(|error| panic!("Fetch fixture partition index: {error}"));
    partition_response.records = records;
    let mut topic_response = FetchableTopicResponse::default();
    topic_response.topic = topic.into();
    topic_response.partitions = vec![partition_response];
    let mut response = WireFetchResponse::default();
    response.throttle_time_ms = 7;
    response.responses = vec![topic_response];
    response
}

/// Encodes one data record at the requested base offset.
pub(crate) fn encoded_data_batch_for_test(base_offset: i64) -> Bytes {
    let batch = RecordBatch {
        base_offset,
        last_offset_delta: 0,
        partition_leader_epoch: -1,
        compression: Compression::None,
        timestamp_type: TimestampType::CreateTime,
        is_transactional: false,
        is_control: false,
        has_delete_horizon: false,
        base_timestamp: 20,
        max_timestamp: 20,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: vec![Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: None,
            value: Some(Bytes::from_static(b"value")),
            headers: Vec::new(),
        }],
    };
    let mut encoded = BytesMut::new();
    batch
        .encode_into(&mut encoded, RecordEncodeLimits::default())
        .unwrap_or_else(|error| panic!("encode Fetch fixture batch: {error}"));
    encoded.freeze()
}
