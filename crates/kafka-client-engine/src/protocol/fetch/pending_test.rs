//! Pending evidence for the additive wire decode-next seam.

use bytes::BytesMut;
use kafka_wire_records::Compression;

use super::{
    FetchDecodeLimits,
    decode_test::{partition, record_bytes, record_bytes_at, response, topic},
    normalize_fetch_response,
};

#[test]
#[ignore = "requires budget-aware kafka-wire-records decode-next retention accounting"]
fn tiny_compressed_batches_share_actual_retained_budget() {
    let first = record_bytes(Compression::Gzip);
    let second = record_bytes_at(Compression::Gzip, 43);
    let mut records = BytesMut::with_capacity(first.len() + second.len());
    records.extend_from_slice(&first);
    records.extend_from_slice(&second);
    let limits = FetchDecodeLimits {
        max_compressed_backing_bytes: 4 * 1024,
        ..FetchDecodeLimits::default()
    };
    let normalized = normalize_fetch_response(
        response(vec![topic(
            "compressed",
            vec![partition(0, Some(records.freeze()))],
        )]),
        limits,
    );
    assert!(
        normalized.is_ok(),
        "tiny batches should debit actual retained backing"
    );
}

#[test]
#[ignore = "requires kafka-wire-records decode-next partial-tail classification"]
fn valid_partial_trailing_batch_is_not_reported_as_corruption() {
    let complete = record_bytes(Compression::None);
    let partial = record_bytes(Compression::None);
    let mut records = BytesMut::with_capacity(complete.len() + 4);
    records.extend_from_slice(&complete);
    records.extend_from_slice(&partial[..4]);
    let normalized = normalize_fetch_response(
        response(vec![topic(
            "partial",
            vec![partition(0, Some(records.freeze()))],
        )]),
        FetchDecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid partial tail must be ignored: {error:?}"));
    assert_eq!(normalized.topics[0].partitions[0].batches.len(), 1);
}
