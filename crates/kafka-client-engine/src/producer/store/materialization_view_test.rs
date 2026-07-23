//! Canonical retention and route validation through store materialization.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{BatchId, OperationId, PartitionIndex};

use super::ProducerStore;
use crate::producer::{
    ProducerStoreError, ProducerStoreLimits,
    record::{ProducerHeader, ProducerRecord},
};

#[test]
fn batch_view_shares_handles_while_the_store_retains_the_canonical_record() {
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
    let mut store = ProducerStore::new(limits());
    let facts = admit(&mut store, record);

    let batch = store
        .materialization_view(BatchId::from_raw(1), 1_024)
        .unwrap_or_else(|error| panic!("materialization view failed: {error}"));
    let (view_topic, partition, records, _limit) = batch.into_parts();
    let (_timestamp, view_key, view_value, view_headers) = records
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("view record missing"))
        .into_parts();
    let (view_name, view_header_value) = view_headers
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("view header missing"))
        .into_parts();

    assert!(Arc::ptr_eq(&view_topic, &topic));
    assert_eq!(partition, 2);
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
    assert!(store.records.record(facts.payload_id()).is_ok());

    assert_eq!(store.release_batch(BatchId::from_raw(1)), Ok(()));
    assert_eq!(
        store.release_payload(facts.payload_id(), facts.retained_bytes()),
        Ok(())
    );
}

#[test]
fn unrepresentable_partition_fails_before_any_record_is_viewed() {
    let mut store = ProducerStore::new(limits());
    let record = ProducerRecord::new(
        Arc::from("orders"),
        PartitionIndex::from_raw(u32::MAX),
        10,
        None,
        Some(Bytes::from_static(b"a")),
    );
    let facts = admit(&mut store, record);
    let before = store.stats();

    assert_eq!(
        store.materialization_view(BatchId::from_raw(1), 1_024),
        Err(ProducerStoreError::PartitionOutOfRange)
    );
    assert_eq!(store.stats(), before);
    assert!(store.records.record(facts.payload_id()).is_ok());

    assert_eq!(store.release_batch(BatchId::from_raw(1)), Ok(()));
    assert_eq!(
        store.release_payload(facts.payload_id(), facts.retained_bytes()),
        Ok(())
    );
}

fn admit(store: &mut ProducerStore, record: ProducerRecord) -> kafka_client_core::ExplicitRecord {
    let reservation = store
        .reserve(record)
        .unwrap_or_else(|error| panic!("reserve failed: {error}"));
    let facts = store
        .commit(reservation)
        .unwrap_or_else(|error| panic!("commit failed: {error}"));
    store
        .accumulate(
            BatchId::from_raw(1),
            OperationId::from_raw(1),
            facts.payload_id(),
        )
        .unwrap_or_else(|error| panic!("accumulate failed: {error}"));
    facts
}

const fn limits() -> ProducerStoreLimits {
    ProducerStoreLimits {
        records: 1,
        bytes: 1_024,
        batches: 1,
    }
}
