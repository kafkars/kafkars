//! Private bridge shape evidence for lease-preserving owned batches and records.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;

use super::{
    AssignedConsumerOwnedBatch, AssignedConsumerOwnedRecord, AssignedConsumerOwnedRecords,
    owned_batch::{record_from_reserved_shared_delivery_parts, reserve_transfer_headers},
};
use crate::header_name::SourceOwner;

type BridgeTransferResult =
    Result<(crate::Record, AssignedConsumerOwnedRecord), (AssignedConsumerOwnedRecord, Arc<str>)>;

#[test]
fn owned_bridge_capabilities_are_send_and_consume_linearly() {
    fn require_send<T: Send>() {}
    fn require_records(_records: fn(AssignedConsumerOwnedBatch) -> AssignedConsumerOwnedRecords) {}
    fn require_record_transfer(
        _transfer: fn(AssignedConsumerOwnedRecord, Arc<str>) -> BridgeTransferResult,
    ) {
    }

    require_send::<AssignedConsumerOwnedBatch>();
    require_send::<AssignedConsumerOwnedRecords>();
    require_send::<AssignedConsumerOwnedRecord>();
    require_records(AssignedConsumerOwnedBatch::into_records);
    require_record_transfer(AssignedConsumerOwnedRecord::try_into_record);
}

#[test]
fn reserved_conversion_and_producer_rejection_preserve_shared_bytes_and_source_lease() {
    let dropped = Arc::new(AtomicBool::new(false));
    let source_owner: Arc<dyn Send + Sync> = Arc::new(DropSentinel(Arc::clone(&dropped)));
    let key = Bytes::from_static(b"key");
    let value = Bytes::from_static(b"value");
    let header_name = Bytes::from("trace".to_owned());
    let header_value = Bytes::from_static(b"header-value");
    let mut reserved_headers = Vec::new();
    reserved_headers
        .try_reserve_exact(3)
        .unwrap_or_else(|error| panic!("reserve test headers: {error}"));
    let record = record_from_reserved_shared_delivery_parts(
        Arc::from("destination"),
        Some(71),
        Some(key.clone()),
        Some(value.clone()),
        vec![
            (header_name.clone(), None),
            (header_name.clone(), Some(Bytes::new())),
            (header_name.clone(), Some(header_value.clone())),
        ]
        .into_iter(),
        SourceOwner::new(source_owner),
        reserved_headers,
    );

    assert_record(
        &record,
        key.as_ptr(),
        value.as_ptr(),
        header_name.as_ptr(),
        header_value.as_ptr(),
    );
    assert!(!dropped.load(Ordering::Acquire));

    let engine = crate::bridge::producer::into_engine_record(record);
    assert_eq!(
        engine.key_bytes().map(|bytes| bytes.as_ptr()),
        Some(key.as_ptr())
    );
    assert_eq!(
        engine.value_bytes().map(|bytes| bytes.as_ptr()),
        Some(value.as_ptr())
    );
    assert!(
        engine
            .headers()
            .iter()
            .all(|header| header.name().as_bytes().as_ptr() == header_name.as_ptr())
    );
    assert_eq!(
        engine.headers()[2].value().map(|bytes| bytes.as_ptr()),
        Some(header_value.as_ptr())
    );
    assert!(!dropped.load(Ordering::Acquire));

    let returned = crate::bridge::producer::restore_rejected_record(engine);
    assert_record(
        &returned,
        key.as_ptr(),
        value.as_ptr(),
        header_name.as_ptr(),
        header_value.as_ptr(),
    );
    assert!(!dropped.load(Ordering::Acquire));
    drop(returned);
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn allocation_rejection_returns_the_exact_source_and_target_owners() {
    let source = Arc::new(());
    let target: Arc<str> = Arc::from("destination");
    let rejected = reserve_transfer_headers(Arc::clone(&source), Arc::clone(&target), usize::MAX);
    let Err((returned_source, returned_target)) = rejected else {
        panic!("capacity overflow must reject before transfer");
    };

    assert!(Arc::ptr_eq(&returned_source, &source));
    assert!(Arc::ptr_eq(&returned_target, &target));
}

fn assert_record(
    record: &crate::Record,
    key_pointer: *const u8,
    value_pointer: *const u8,
    header_name_pointer: *const u8,
    header_value_pointer: *const u8,
) {
    assert_eq!(record.topic(), "destination");
    assert_eq!(record.explicit_partition(), None);
    assert_eq!(record.timestamp(), Some(71));
    assert_eq!(
        record.key_bytes().map(|bytes| bytes.as_ptr()),
        Some(key_pointer)
    );
    assert_eq!(
        record.value_bytes().map(|bytes| bytes.as_ptr()),
        Some(value_pointer)
    );
    assert_eq!(record.headers().len(), 3);
    assert!(
        record
            .headers()
            .iter()
            .all(|header| header.name() == "trace")
    );
    assert!(
        record
            .headers()
            .iter()
            .all(|header| { header.header_name().as_bytes().as_ptr() == header_name_pointer })
    );
    assert_eq!(record.headers()[0].value(), None);
    assert_eq!(record.headers()[1].value(), Some(&Bytes::new()));
    assert_eq!(
        record.headers()[2].value().map(|bytes| bytes.as_ptr()),
        Some(header_value_pointer)
    );
}

struct DropSentinel(Arc<AtomicBool>);

impl Drop for DropSentinel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
