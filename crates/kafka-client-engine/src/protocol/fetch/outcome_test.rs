//! Shared fixtures and retained-payload evidence for composed Fetch outcomes.

use bytes::{Bytes, BytesMut};
use kafka_wire::{
    FetchResponse as WireFetchResponse,
    fetch_response::{FetchableTopicResponse, PartitionData},
};
use kafka_wire_records::{RecordBatch, RecordEncodeLimits};

use super::{
    FetchDecodeLimits, FetchOutputReservation, FetchRetentionFailure, batch_model_test::batch,
    normalize_read_uncommitted_fetch_outcome,
};

pub(super) const TOPIC: &str = "events";
pub(super) const PARTITION: u32 = 3;
pub(super) const REQUESTED_OFFSET: i64 = 10;
pub(super) const SELECTED_VERSION: i16 = 12;

#[test]
fn retained_uncompressed_payload_moves_without_copying() {
    let (response, encoded) = response_with_data_then_control();
    let normalized = normalize(response, REQUESTED_OFFSET, usize::MAX)
        .unwrap_or_else(|rejected| panic!("retained outcome: {:?}", rejected.failure()));
    let batches = normalized
        .outcome()
        .data_batches()
        .unwrap_or_else(|| panic!("successful data"));

    assert_eq!(normalized.throttle_ticks(), Some(7_000_000));
    assert_eq!(normalized.outcome().next_offset(), Some(21));
    assert_eq!(batches.len(), 1);
    assert!(batches[0].is_transactional);
    let value = batches[0].records[0]
        .value
        .as_ref()
        .unwrap_or_else(|| panic!("data record value"));
    let start = encoded.as_ptr() as usize;
    let end = start + encoded.len();
    let retained = value.as_ptr() as usize;
    assert!(
        (start..end).contains(&retained),
        "uncompressed value must remain a response-byte slice"
    );
}

#[test]
fn scratch_decode_limits_are_independent_from_final_output_reservation() {
    let (response, _) = response_with_data_then_control();
    let normalized = normalize(response, REQUESTED_OFFSET, usize::MAX)
        .unwrap_or_else(|rejected| panic!("measure exact charge: {:?}", rejected.failure()));
    let exact = normalized.retained_bytes();
    assert!(exact > 0);
    assert_eq!(normalized.unused_reserved_bytes(), usize::MAX - exact);

    let (response, _) = response_with_data_then_control();
    let Err(rejected) = normalize(response, REQUESTED_OFFSET, exact - 1) else {
        panic!("decode may succeed, but an undersized final reservation must fail");
    };
    let (failure, reservation) = rejected.into_parts();
    assert_eq!(
        failure,
        super::FetchOutcomeFailure::Retention(FetchRetentionFailure::ReservationExceeded {
            actual: exact,
            reserved: exact - 1,
        })
    );
    assert_eq!(reservation.bytes(), exact - 1);
}

pub(super) fn normalize(
    response: WireFetchResponse,
    requested_offset: i64,
    reserved_bytes: usize,
) -> Result<super::RetainedFetchOutcome, super::RejectedFetchOutcome> {
    normalize_with(
        response,
        requested_offset,
        SELECTED_VERSION,
        FetchDecodeLimits::default(),
        reserved_bytes,
    )
}

pub(super) fn normalize_with(
    response: WireFetchResponse,
    requested_offset: i64,
    selected_version: i16,
    limits: FetchDecodeLimits,
    reserved_bytes: usize,
) -> Result<super::RetainedFetchOutcome, super::RejectedFetchOutcome> {
    normalize_read_uncommitted_fetch_outcome(
        TOPIC,
        PARTITION,
        requested_offset,
        selected_version,
        response,
        limits,
        FetchOutputReservation::from_acquired_capacity(reserved_bytes),
    )
}

pub(super) fn response(records: Option<Bytes>) -> WireFetchResponse {
    response_with_partition(partition(0, records))
}

pub(super) fn response_with_partition(partition: PartitionData) -> WireFetchResponse {
    let mut topic = FetchableTopicResponse::default();
    topic.topic = TOPIC.into();
    topic.partitions = vec![partition];
    let mut response = WireFetchResponse::default();
    response.throttle_time_ms = 7;
    response.responses = vec![topic];
    response
}

pub(super) fn partition(error_code: i16, records: Option<Bytes>) -> PartitionData {
    let mut partition = PartitionData::default();
    partition.partition_index =
        i32::try_from(PARTITION).unwrap_or_else(|error| panic!("test partition: {error}"));
    partition.error_code = error_code;
    partition.records = records;
    partition
}

pub(super) fn response_with_data_then_control() -> (WireFetchResponse, Bytes) {
    let mut data = batch();
    data.base_offset = 10;
    data.producer_id = 7;
    data.producer_epoch = 2;
    data.base_sequence = 0;
    data.is_transactional = true;
    let mut control = batch();
    control.base_offset = 20;
    control.is_control = true;
    let encoded = encoded_batches(&[data, control]);
    (response(Some(encoded.clone())), encoded)
}

pub(super) fn encoded_batches(batches: &[RecordBatch]) -> Bytes {
    let mut encoded = BytesMut::new();
    for batch in batches {
        batch
            .encode_into(&mut encoded, RecordEncodeLimits::default())
            .unwrap_or_else(|error| panic!("test batch encoding: {error}"));
    }
    encoded.freeze()
}
