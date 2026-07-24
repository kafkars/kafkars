//! Requested-offset filtering and complete-versus-partial batch progression.

use bytes::BytesMut;
use kafka_wire_records::Compression;

use super::{
    batch_model_test::batch,
    decode_test::{batch_bytes, record_bytes_at},
    outcome_test::{encoded_batches, normalize, response},
};

#[test]
fn overlapping_first_batch_delivers_only_records_at_or_above_requested_offset() {
    let normalized = normalize(
        response(Some(record_bytes_at(Compression::None, 40))),
        41,
        usize::MAX,
    )
    .unwrap_or_else(|rejected| panic!("overlapping batch: {:?}", rejected.failure()));
    let batches = normalized
        .outcome()
        .data_batches()
        .unwrap_or_else(|| panic!("successful batches"));

    assert_eq!(normalized.outcome().next_offset(), Some(43));
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].records.len(), 1);
    assert_eq!(batches[0].records[0].offset, 42);
}

#[test]
fn entirely_stale_batch_is_dropped_and_cannot_regress_next_offset() {
    let normalized = normalize(
        response(Some(record_bytes_at(Compression::None, 40))),
        50,
        0,
    )
    .unwrap_or_else(|rejected| panic!("stale batch: {:?}", rejected.failure()));
    let batches = normalized
        .outcome()
        .data_batches()
        .unwrap_or_else(|| panic!("successful batches"));

    assert_eq!(normalized.outcome().next_offset(), Some(50));
    assert!(batches.is_empty());
    assert_eq!(normalized.retained_bytes(), 0);
}

#[test]
fn complete_control_batch_advances_without_application_delivery() {
    let mut control = batch();
    control.base_offset = 20;
    control.is_control = true;
    let normalized = normalize(response(Some(batch_bytes(&control))), 10, 0)
        .unwrap_or_else(|rejected| panic!("control progression: {:?}", rejected.failure()));
    let batches = normalized
        .outcome()
        .data_batches()
        .unwrap_or_else(|| panic!("successful batches"));

    assert_eq!(normalized.outcome().next_offset(), Some(21));
    assert!(batches.is_empty());
    assert_eq!(normalized.retained_bytes(), 0);
}

#[test]
fn partial_trailing_batch_never_advances_beyond_final_complete_batch() {
    let complete = record_bytes_at(Compression::None, 40);
    let partial = record_bytes_at(Compression::None, 50);
    let mut records = BytesMut::with_capacity(complete.len() + 4);
    records.extend_from_slice(&complete);
    records.extend_from_slice(&partial[..4]);
    let normalized = normalize(response(Some(records.freeze())), 40, usize::MAX)
        .unwrap_or_else(|rejected| panic!("partial tail: {:?}", rejected.failure()));

    assert_eq!(normalized.outcome().next_offset(), Some(43));
    assert_eq!(
        normalized
            .outcome()
            .data_batches()
            .unwrap_or_else(|| panic!("successful batches"))
            .len(),
        1
    );
}

#[test]
fn complete_empty_data_batch_advances_without_delivery_or_charge() {
    let mut empty = batch();
    empty.base_offset = 30;
    empty.base_timestamp = -1;
    empty.records.clear();
    let normalized = normalize(response(Some(encoded_batches(&[empty]))), 10, 0)
        .unwrap_or_else(|rejected| panic!("empty compacted batch: {:?}", rejected.failure()));

    assert_eq!(normalized.outcome().next_offset(), Some(31));
    assert!(
        normalized
            .outcome()
            .data_batches()
            .unwrap_or_else(|| panic!("successful batches"))
            .is_empty()
    );
    assert_eq!(normalized.retained_bytes(), 0);
}
