//! Materialization-view scenarios for canonical record ownership.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::{
    ProducerStoreError,
    record::{ProducerHeader, ProducerRecord},
    record_store::RecordStore,
};

#[test]
fn materialization_view_keeps_canonical_record_owned_until_terminal_release() {
    let topic: Arc<str> = Arc::from("orders");
    let key = Bytes::from_static(b"key");
    let value = Bytes::from_static(b"value");
    let header_value = Bytes::from_static(b"header-value");
    let record = ProducerRecord::new(
        Arc::clone(&topic),
        PartitionIndex::from_raw(2),
        100,
        Some(key.clone()),
        Some(value.clone()),
    )
    .with_headers(vec![ProducerHeader::new(
        "traceparent".to_owned(),
        Some(header_value.clone()),
    )]);
    let mut records = RecordStore::new(1, 1_024);
    let reservation = records
        .reserve(record)
        .unwrap_or_else(|error| panic!("reservation failed: {error}"));
    let facts = reservation.facts();
    records
        .commit(reservation)
        .unwrap_or_else(|error| panic!("commit failed: {error}"));

    let view = records
        .record(facts.payload_id())
        .unwrap_or_else(|error| panic!("canonical record missing: {error}"))
        .materialization_view();
    let (_timestamp, view_key, view_value, view_headers) = view.into_parts();
    let (view_name, view_header_value) = view_headers
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("view header missing"))
        .into_parts();

    assert_eq!(
        view_key.as_ref().map(|bytes| bytes.as_ptr()),
        Some(key.as_ptr())
    );
    assert_eq!(
        view_value.as_ref().map(|bytes| bytes.as_ptr()),
        Some(value.as_ptr())
    );
    assert_eq!(view_name.as_ref(), b"traceparent");
    assert_eq!(
        view_header_value.as_ref().map(|bytes| bytes.as_ptr()),
        Some(header_value.as_ptr())
    );
    assert!(records.record(facts.payload_id()).is_ok());

    assert_eq!(
        records.release(facts.payload_id(), facts.retained_bytes()),
        Ok(())
    );
    assert_eq!(
        records.record(facts.payload_id()),
        Err(ProducerStoreError::UnknownPayload)
    );
}
