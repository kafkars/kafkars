//! Read-committed visibility, marker retirement, and LSO fence scenarios.

use bytes::{Bytes, BytesMut};
use kafka_wire::{ControlRecordTypeSchema, EndTxnMarker};
use kafka_wire_core::{ApiVersion, Encoder, KafkaEncode};

use super::{
    FetchBatch, FetchDecodeFailure, FetchHeader, FetchPartition, FetchProducerIdentity,
    FetchRecord, FetchResponse, FetchTimestampType, FetchTopic, model::FetchAbortedTransaction,
    read_committed::filter_read_committed,
};

#[test]
fn aborted_ranges_hide_records_and_abort_retires_only_the_earliest_match() {
    let mut response = response(
        100,
        vec![(8, 10), (8, 30)],
        vec![
            data_batch(9, 10, 8),
            marker_batch(12, 8, 0),
            data_batch(20, 20, 8),
            data_batch(30, 30, 8),
            marker_batch(31, 8, 1),
            data_batch(32, 32, 8),
            marker_batch(33, 8, 0),
            data_batch(34, 34, 8),
        ],
    );

    assert_eq!(filter_read_committed(&mut response), Ok(()));
    let batches = &response.topics[0].partitions[0].batches;
    assert!(batches[0].records.is_empty());
    assert_eq!(batches[2].records.len(), 1);
    assert!(batches[3].records.is_empty());
    assert!(batches[5].records.is_empty());
    assert_eq!(batches[7].records.len(), 1);
    assert_eq!(batches[0].next_offset, 11);
}

#[test]
fn producer_without_an_active_aborted_range_remains_visible() {
    let mut response = response(
        50,
        vec![(7, 10)],
        vec![
            data_batch(10, 10, 8),
            data_batch(11, 11, 7),
            marker_batch(12, 7, 0),
            data_batch(13, 13, 7),
        ],
    );

    assert_eq!(filter_read_committed(&mut response), Ok(()));
    let batches = &response.topics[0].partitions[0].batches;
    assert_eq!(batches[0].records.len(), 1);
    assert!(batches[1].records.is_empty());
    assert_eq!(batches[3].records.len(), 1);
}

#[test]
fn last_stable_offset_is_required_and_empty_aborted_ranges_are_valid() {
    let mut missing = response(10, Vec::new(), Vec::new());
    missing.topics[0].partitions[0].last_stable_offset = None;
    assert_eq!(
        filter_read_committed(&mut missing),
        Err(FetchDecodeFailure::MissingLastStableOffset)
    );

    let mut at_lso = response(10, Vec::new(), vec![data_batch(10, 10, 8)]);
    assert_eq!(
        filter_read_committed(&mut at_lso),
        Err(FetchDecodeFailure::BatchAtOrAfterLastStableOffset {
            last_offset: 10,
            last_stable_offset: 10,
        })
    );

    let mut no_aborted = response(20, Vec::new(), vec![data_batch(7, 7, 8)]);
    assert_eq!(filter_read_committed(&mut no_aborted), Ok(()));
    assert_eq!(
        no_aborted.topics[0].partitions[0].batches[0].records.len(),
        1
    );
}

#[test]
fn unordered_aborted_entries_are_sorted_before_filtering() {
    let mut response = response(
        50,
        vec![(9, 30), (8, 10), (8, 10)],
        vec![
            data_batch(10, 10, 8),
            marker_batch(11, 8, 0),
            data_batch(30, 30, 9),
            marker_batch(31, 9, 0),
        ],
    );

    assert_eq!(filter_read_committed(&mut response), Ok(()));
    let partition = &response.topics[0].partitions[0];
    assert_eq!(partition.aborted_transactions[0].first_offset, 10);
    assert_eq!(partition.aborted_transactions[1].first_offset, 30);
    assert_eq!(partition.aborted_transactions.len(), 2);
    assert!(partition.batches[0].records.is_empty());
    assert!(partition.batches[2].records.is_empty());
}

#[test]
fn unknown_transaction_control_type_is_invalid() {
    let mut response = response(20, vec![(8, 5)], vec![marker_batch(9, 8, 7)]);
    assert_eq!(
        filter_read_committed(&mut response),
        Err(FetchDecodeFailure::UnsupportedControlRecordType { actual: 7 })
    );
}

#[test]
fn nontransactional_control_batch_is_invalid() {
    let mut control = marker_batch(9, 8, 0);
    control.is_transactional = false;
    control.producer = None;
    let mut response = response(20, Vec::new(), vec![control]);
    assert_eq!(
        filter_read_committed(&mut response),
        Err(FetchDecodeFailure::NonTransactionalControlIdentity)
    );
}

fn response(
    last_stable_offset: i64,
    aborted: Vec<(i64, i64)>,
    batches: Vec<FetchBatch>,
) -> FetchResponse {
    FetchResponse {
        throttle_time_ms: 0,
        error_code: 0,
        session_id: 0,
        topics: vec![FetchTopic {
            name: Bytes::from_static(b"orders"),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                index: 0,
                error_code: 0,
                high_watermark: Some(last_stable_offset),
                last_stable_offset: Some(last_stable_offset),
                log_start_offset: Some(0),
                diverging_epoch: None,
                current_leader: None,
                snapshot_id: None,
                preferred_read_replica: None,
                aborted_transactions: aborted
                    .into_iter()
                    .map(|(producer_id, first_offset)| FetchAbortedTransaction {
                        producer_id,
                        first_offset,
                    })
                    .collect(),
                batches,
            }],
        }],
        endpoints: Vec::new(),
    }
}

fn data_batch(base_offset: i64, last_offset: i64, producer_id: i64) -> FetchBatch {
    batch(
        base_offset,
        last_offset,
        producer_id,
        false,
        vec![record(last_offset, None, None)],
    )
}

fn marker_batch(offset: i64, producer_id: i64, control_type: i16) -> FetchBatch {
    batch(
        offset,
        offset,
        producer_id,
        true,
        vec![record(
            offset,
            Some(marker_key(control_type)),
            Some(marker_value()),
        )],
    )
}

fn batch(
    base_offset: i64,
    last_offset: i64,
    producer_id: i64,
    is_control: bool,
    records: Vec<FetchRecord>,
) -> FetchBatch {
    FetchBatch {
        base_offset,
        last_offset,
        next_offset: last_offset + 1,
        partition_leader_epoch: None,
        timestamp_type: FetchTimestampType::Create,
        max_timestamp: Some(1),
        producer: Some(FetchProducerIdentity {
            producer_id,
            producer_epoch: 1,
            base_sequence: if is_control { -1 } else { 0 },
        }),
        is_transactional: true,
        is_control,
        delete_horizon_ms: None,
        records,
    }
}

fn record(offset: i64, key: Option<Bytes>, value: Option<Bytes>) -> FetchRecord {
    FetchRecord {
        attributes: 0,
        offset,
        timestamp: Some(1),
        key,
        value,
        headers: Vec::<FetchHeader>::new(),
    }
}

fn marker_key(control_type: i16) -> Bytes {
    let mut bytes = BytesMut::new();
    let mut encoder = Encoder::new(&mut bytes);
    encoder
        .write_i16(0)
        .unwrap_or_else(|error| panic!("control key version: {error}"));
    let mut key = ControlRecordTypeSchema::default();
    key.type_ = control_type;
    key.encode(&mut encoder, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("generated control key: {error}"));
    bytes.freeze()
}

fn marker_value() -> Bytes {
    let mut bytes = BytesMut::new();
    let mut encoder = Encoder::new(&mut bytes);
    encoder
        .write_i16(0)
        .unwrap_or_else(|error| panic!("marker value version: {error}"));
    EndTxnMarker::default()
        .encode(&mut encoder, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("generated marker value: {error}"));
    bytes.freeze()
}
