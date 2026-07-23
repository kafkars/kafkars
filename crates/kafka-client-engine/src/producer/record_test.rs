//! Record accounting scenarios for byte-backed header metadata.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::{
    ProducerStore, ProducerStoreError, ProducerStoreLimits,
    record::{HEADER_CONTROL_BYTES, ProducerHeader, ProducerRecord, header_control_bytes},
};

#[test]
fn validated_header_name_round_trips_exact_utf8_value() {
    let expected = "trace-\u{fffd}-é";
    let name = expected.to_owned();
    let header = ProducerHeader::new(name, None);
    let view = header.materialization_view();
    let (view_name, _value) = view.into_parts();

    assert_eq!(view_name.as_ref(), expected.as_bytes());
    drop(view_name);

    let (returned, _value) = header.into_parts();
    assert_eq!(returned, expected);
}

#[test]
fn empty_headers_charge_their_control_storage_before_admission() {
    let oversized_empty = String::with_capacity(1024 * 1024);
    assert_eq!(oversized_empty.len(), 0);
    let headers = vec![
        ProducerHeader::new(oversized_empty, None),
        ProducerHeader::new(String::new(), None),
    ];
    let record = ProducerRecord::new(
        Arc::from("orders"),
        PartitionIndex::from_raw(0),
        1,
        None,
        None,
    )
    .with_headers(headers);
    let expected = "orders".len() + (2 * HEADER_CONTROL_BYTES);
    assert_eq!(record.retained_bytes(), Ok(expected));

    let mut store = ProducerStore::new(ProducerStoreLimits {
        records: 1,
        bytes: expected - 1,
        batches: 1,
    });
    let Err(rejected) = store.reserve(record) else {
        panic!("empty header controls must not bypass the byte bound");
    };
    assert_eq!(rejected.reason(), ProducerStoreError::ByteCapacity);
    assert_eq!(store.stats().bytes, 0);
}

#[test]
fn header_count_and_control_size_are_checked_before_accounting() {
    let over_protocol_count = usize::try_from(i32::MAX)
        .unwrap_or_else(|_| panic!("i32::MAX must fit usize"))
        .saturating_add(1);
    assert_eq!(
        header_control_bytes(over_protocol_count, over_protocol_count),
        Err(ProducerStoreError::HeaderCountOutOfRange)
    );
    assert_eq!(
        header_control_bytes(0, usize::MAX),
        Err(ProducerStoreError::RetainedSizeOverflow)
    );
    assert!(HEADER_CONTROL_BYTES >= std::mem::size_of::<Bytes>());
}
