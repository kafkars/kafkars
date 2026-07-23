//! Materialization transfer preserves record order and all nullable fields.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{BatchId, OperationId, PartitionIndex};

use crate::protocol::produce::materialize_explicit_produce_batch;

use super::{
    ProducerHeader, ProducerRecord, ProducerStore, ProducerStoreError, ProducerStoreLimits,
};

#[test]
fn ordered_records_null_empty_and_duplicate_headers_survive_transfer() {
    let mut store = ProducerStore::new(limits());
    let first = admit(
        &mut store,
        record(100, None, None, Vec::new()),
        OperationId::from_raw(1),
    );
    let second = admit(
        &mut store,
        record(
            101,
            Some(Bytes::new()),
            Some(Bytes::new()),
            vec![
                ProducerHeader::new("traceparent".to_owned(), Some(Bytes::from_static(b"first"))),
                ProducerHeader::new("traceparent".to_owned(), None),
                ProducerHeader::new("traceparent".to_owned(), Some(Bytes::new())),
            ],
        ),
        OperationId::from_raw(2),
    );
    let batch = store
        .take_materialization(BatchId::from_raw(1), 30_000, 1_048_576)
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    let (topic, partition, records, timeout, limit) = batch.into_parts();

    assert_eq!(topic, "orders");
    assert_eq!(partition, 7);
    assert_eq!(timeout, 30_000);
    assert_eq!(limit, 1_048_576);
    assert_eq!(records.len(), 2);
    let mut records = records.into_iter();
    let (first_timestamp, first_key, first_value, first_headers) = records
        .next()
        .unwrap_or_else(|| panic!("first record missing"))
        .into_parts();
    let (second_timestamp, second_key, second_value, second_headers) = records
        .next()
        .unwrap_or_else(|| panic!("second record missing"))
        .into_parts();
    assert_eq!(first_timestamp, 100);
    assert!(first_key.is_none());
    assert!(first_value.is_none());
    assert!(first_headers.is_empty());
    assert_eq!(second_timestamp, 101);
    assert_eq!(second_key.as_deref(), Some(&b""[..]));
    assert_eq!(second_value.as_deref(), Some(&b""[..]));
    let headers = second_headers
        .into_iter()
        .map(super::MaterializationHeader::into_parts)
        .collect::<Vec<_>>();
    assert_eq!(headers[0].0, "traceparent");
    assert_eq!(headers[0].1.as_deref(), Some(&b"first"[..]));
    assert_eq!(headers[1].0, "traceparent");
    assert!(headers[1].1.is_none());
    assert_eq!(headers[2].0, "traceparent");
    assert_eq!(headers[2].1.as_deref(), Some(&b""[..]));

    assert_eq!(store.release_batch(BatchId::from_raw(1)), Ok(()));
    assert_eq!(
        store.release_payload(first.payload_id(), first.retained_bytes()),
        Ok(())
    );
    assert_eq!(
        store.release_payload(second.payload_id(), second.retained_bytes()),
        Ok(())
    );
}

#[test]
fn member_removal_preserves_sibling_admission_order() {
    let mut store = ProducerStore::new(limits());
    let first = admit(
        &mut store,
        record(1, None, Some(Bytes::from_static(b"a")), Vec::new()),
        OperationId::from_raw(1),
    );
    let second = admit(
        &mut store,
        record(2, None, Some(Bytes::from_static(b"b")), Vec::new()),
        OperationId::from_raw(2),
    );
    let third = admit(
        &mut store,
        record(3, None, Some(Bytes::from_static(b"c")), Vec::new()),
        OperationId::from_raw(3),
    );
    assert_eq!(
        store.remove_member(BatchId::from_raw(1), OperationId::from_raw(2)),
        Ok(second.payload_id())
    );

    let batch = store
        .take_materialization(BatchId::from_raw(1), 1_000, 1_024)
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    let records = batch.into_parts().2;
    let values = records
        .into_iter()
        .map(|record| record.into_parts().2)
        .collect::<Vec<_>>();
    assert_eq!(values[0].as_deref(), Some(&b"a"[..]));
    assert_eq!(values[1].as_deref(), Some(&b"c"[..]));

    assert_eq!(store.release_batch(BatchId::from_raw(1)), Ok(()));
    for facts in [first, second, third] {
        assert_eq!(
            store.release_payload(facts.payload_id(), facts.retained_bytes()),
            Ok(())
        );
    }
}

#[test]
fn unrepresentable_partition_fails_before_any_record_is_moved() {
    let mut store = ProducerStore::new(limits());
    let facts = admit(
        &mut store,
        ProducerRecord::new(
            Arc::from("orders"),
            PartitionIndex::from_raw(u32::MAX),
            10,
            None,
            Some(Bytes::from_static(b"a")),
        ),
        OperationId::from_raw(1),
    );
    let before = store.stats();

    assert_eq!(
        store.take_materialization(BatchId::from_raw(1), 1_000, 1_024),
        Err(ProducerStoreError::PartitionOutOfRange)
    );
    assert_eq!(store.stats(), before);
    assert_eq!(store.release_batch(BatchId::from_raw(1)), Ok(()));
    assert_eq!(
        store.release_payload(facts.payload_id(), facts.retained_bytes()),
        Ok(())
    );
}

#[test]
fn encoding_failure_keeps_accounting_until_ordered_release_effects() {
    let mut store = ProducerStore::new(limits());
    let facts = admit(
        &mut store,
        record(10, None, Some(Bytes::from_static(b"payload")), Vec::new()),
        OperationId::from_raw(1),
    );
    let input = store
        .take_materialization(BatchId::from_raw(1), 1_000, 1)
        .unwrap_or_else(|error| panic!("store transfer failed: {error}"));

    assert!(materialize_explicit_produce_batch(input).is_err());
    assert_eq!(store.stats().records, 1);
    assert_eq!(store.stats().batches, 1);
    assert_eq!(store.release_batch(BatchId::from_raw(1)), Ok(()));
    assert_eq!(
        store.release_payload(facts.payload_id(), facts.retained_bytes()),
        Ok(())
    );
    assert_eq!(store.stats().bytes, 0);
}

fn admit(
    store: &mut ProducerStore,
    record: ProducerRecord,
    operation_id: OperationId,
) -> kafka_client_core::ExplicitRecord {
    let reservation = store
        .reserve(record)
        .unwrap_or_else(|error| panic!("reserve failed: {error}"));
    let facts = store
        .commit(reservation)
        .unwrap_or_else(|error| panic!("commit failed: {error}"));
    store
        .accumulate(BatchId::from_raw(1), operation_id, facts.payload_id())
        .unwrap_or_else(|error| panic!("accumulate failed: {error}"));
    facts
}

fn record(
    timestamp: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<ProducerHeader>,
) -> ProducerRecord {
    ProducerRecord::new(
        Arc::from("orders"),
        PartitionIndex::from_raw(7),
        timestamp,
        key,
        value,
    )
    .with_headers(headers)
}

const fn limits() -> ProducerStoreLimits {
    ProducerStoreLimits {
        records: 8,
        bytes: 1_024,
        batches: 4,
    }
}
