//! Encoded Fetch composition evidence for read-committed terminal outcomes.

use bytes::{Bytes, BytesMut};
use kafka_wire::{ControlRecordTypeSchema, EndTxnMarker, fetch_response::AbortedTransaction};
use kafka_wire_core::{ApiVersion, Encoder, KafkaEncode};
use kafka_wire_records::{Record, RecordBatch};

use super::{
    FetchDecodeLimits, FetchIsolation, normalize_fetch_outcome,
    outcome_test::{
        PARTITION, REQUESTED_OFFSET, SELECTED_VERSION, encoded_batches, partition,
        response_with_partition,
    },
    retention::FetchReservationDomain,
};

#[test]
fn encoded_aborted_data_is_hidden_while_complete_progress_advances() {
    let mut aborted_data = application_batch(10);
    aborted_data.is_transactional = true;
    aborted_data.producer_id = 7;
    aborted_data.producer_epoch = 2;
    aborted_data.base_sequence = 0;

    let mut abort_marker = application_batch(11);
    abort_marker.is_transactional = true;
    abort_marker.is_control = true;
    abort_marker.producer_id = 7;
    abort_marker.producer_epoch = 2;
    abort_marker.base_sequence = -1;
    abort_marker.records = vec![Record {
        attributes: 0,
        timestamp_delta: 0,
        offset_delta: 0,
        key: Some(marker_key(0)),
        value: Some(marker_value()),
        headers: Vec::new(),
    }];
    let committed_data = application_batch(12);

    let mut partition = partition(
        0,
        Some(encoded_batches(&[
            aborted_data,
            abort_marker,
            committed_data,
        ])),
    );
    partition.high_watermark = 20;
    partition.last_stable_offset = 20;
    let mut aborted = AbortedTransaction::default();
    aborted.producer_id = 7;
    aborted.first_offset = 10;
    partition.aborted_transactions = Some(vec![aborted]);
    let domain = FetchReservationDomain::create_store_domain();
    let (_proof, reservation) = domain.issue_pair(0, usize::MAX);

    let retained = normalize_fetch_outcome(
        FetchIsolation::ReadCommitted,
        super::outcome_test::TOPIC,
        PARTITION,
        REQUESTED_OFFSET,
        SELECTED_VERSION,
        response_with_partition(partition),
        FetchDecodeLimits::default(),
        reservation,
    )
    .unwrap_or_else(|rejected| panic!("read-committed outcome: {:?}", rejected.failure()));
    let batches = retained
        .outcome()
        .data_batches()
        .unwrap_or_else(|| panic!("successful Fetch outcome"));

    assert_eq!(retained.outcome().next_offset(), Some(13));
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].base_offset, 12);
    assert_eq!(
        batches[0].records[0].value.as_deref(),
        Some(&b"visible"[..])
    );
}

fn application_batch(base_offset: i64) -> RecordBatch {
    let mut batch = super::batch_model_test::batch();
    batch.base_offset = base_offset;
    batch.records[0].value = Some(Bytes::from_static(b"visible"));
    batch
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
