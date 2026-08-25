//! Transactional public-record validation and shared-view scenarios.

#![expect(
    clippy::expect_used,
    reason = "test assertions intentionally fail immediately when fixture construction is invalid"
)]

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::{
    PublicProducerHeader as ProducerHeader, PublicProducerRecord as ProducerRecord,
    TransactionRecordViewError,
};

#[test]
fn transactional_view_preserves_one_explicit_route_and_materialization_view() {
    let value = Bytes::from_static(b"value");
    let header_name = Bytes::from("trace".to_owned());
    let header_name_pointer = header_name.as_ptr();
    let view = ProducerRecord::to("orders")
        .partition(3)
        .timestamp_milliseconds(71)
        .key(Bytes::from_static(b"key"))
        .value(value.clone())
        .header(
            ProducerHeader::try_from_shared_name(header_name, Some(Bytes::from_static(b"abc")))
                .expect("valid shared header name"),
        )
        .header(ProducerHeader::null("nullable"))
        .transaction_view(99)
        .expect("valid transactional record");
    let (topic, partition, materialization, retained_bytes) = view.into_parts();
    let (timestamp, key, actual_value, headers) = materialization.into_parts();

    assert_eq!(&*topic, "orders");
    assert_eq!(partition.map(PartitionIndex::get), Some(3));
    assert_eq!(timestamp, 71);
    assert_eq!(key, Some(Bytes::from_static(b"key")));
    assert_eq!(actual_value, Some(value));
    assert!(retained_bytes >= "orders".len() + 3 + 5 + 5 + 3 + 8);
    let mut headers = headers.into_iter();
    let (actual_header_name, actual_header_value) = headers
        .next()
        .map(super::super::materialization::MaterializationHeader::into_parts)
        .expect("first header");
    assert_eq!(actual_header_name.as_ref(), b"trace");
    assert_eq!(actual_header_name.as_ptr(), header_name_pointer);
    assert_eq!(actual_header_value, Some(Bytes::from_static(b"abc")));
    assert_eq!(
        headers
            .next()
            .map(super::super::materialization::MaterializationHeader::into_parts),
        Some((Bytes::from_static(b"nullable"), None))
    );
    assert!(headers.next().is_none());
}

#[test]
fn transactional_view_uses_the_boundary_default_timestamp() {
    let view = ProducerRecord::to("orders")
        .partition(0)
        .transaction_view(1234)
        .expect("valid transactional record");
    let (_, partition, materialization, _) = view.into_parts();

    assert_eq!(partition, Some(PartitionIndex::from_raw(0)));
    assert_eq!(materialization.timestamp_ms_for_protocol(), 1234);
}

#[test]
fn transactional_view_accepts_automatic_and_rejects_invalid_routes() {
    assert_eq!(
        ProducerRecord::to("")
            .partition(0)
            .transaction_view(1)
            .expect_err("empty topic"),
        TransactionRecordViewError::EmptyTopic
    );
    let (_, partition, _, _) = ProducerRecord::to("orders")
        .transaction_view(1)
        .expect("automatic partitioning remains unresolved")
        .into_parts();
    assert_eq!(partition, None);
    assert_eq!(
        ProducerRecord::to("orders")
            .partition(-1)
            .transaction_view(1)
            .expect_err("negative explicit partition"),
        TransactionRecordViewError::NegativeExplicitPartition
    );
}

#[test]
fn header_reservation_failure_leaves_the_original_record_untouched() {
    let key = Bytes::from_static(b"key");
    let header_name = Bytes::from_static(b"trace");
    let header_name_pointer = header_name.as_ptr();
    let header_value = Bytes::from_static(b"value");
    let source_owner = Arc::new(());
    let record = ProducerRecord::to("orders")
        .partition(2)
        .key(key.clone())
        .header(
            ProducerHeader::try_from_shared_name(header_name.clone(), Some(header_value.clone()))
                .expect("valid shared header name"),
        )
        .retain_source_owner(source_owner.clone());
    let source_owner_count = Arc::strong_count(&source_owner);

    assert_eq!(
        record
            .transaction_view_with_header_capacity_for_test(71, usize::MAX)
            .expect_err("impossible header capacity must reject"),
        TransactionRecordViewError::Allocation
    );
    assert_eq!(record.topic(), "orders");
    assert_eq!(record.explicit_partition(), Some(2));
    assert_eq!(
        record.key_bytes().map(|bytes| bytes.as_ptr()),
        Some(key.as_ptr())
    );
    assert_eq!(record.headers().len(), 1);
    assert_eq!(Arc::strong_count(&source_owner), source_owner_count);
    assert_eq!(record.headers()[0].name(), "trace");
    assert_eq!(record.headers()[0].name().as_ptr(), header_name_pointer);
    assert_eq!(record.headers()[0].value(), Some(&header_value));
    assert_eq!(
        record.headers()[0].value().map(|bytes| bytes.as_ptr()),
        Some(header_value.as_ptr())
    );
}
