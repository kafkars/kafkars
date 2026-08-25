//! Fallible facade-record mirroring and exact-owner recovery evidence.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;

use super::conversion::{prepare_engine_record, prepare_engine_record_with_header_capacity};
use crate::{
    HeaderName,
    header_name::SourceOwner,
    record::{Header, Record, RecordTransferParts},
};

#[test]
fn prepared_mirror_preserves_original_and_shared_record_owners() {
    let topic: Arc<str> = Arc::from("orders");
    let key = Bytes::from_static(b"key");
    let header_name_bytes = Bytes::from("trace".to_owned());
    let header_name = HeaderName::try_from_bytes(header_name_bytes.clone())
        .unwrap_or_else(|error| panic!("valid header name: {error}"));
    let retained = Bytes::from_static(b"last");
    let dropped = Arc::new(AtomicBool::new(false));
    let source_owner: Arc<dyn Send + Sync> = Arc::new(DropSentinel(Arc::clone(&dropped)));
    let topic_uuid =
        crate::TopicUuid::try_from_bytes([7; 16]).unwrap_or_else(|| panic!("nonzero topic UUID"));
    let original = Record::from_transfer_parts(RecordTransferParts {
        topic: Arc::clone(&topic),
        expected_topic_uuid: Some(topic_uuid),
        partition: None,
        timestamp_milliseconds: None,
        key: Some(key.clone()),
        value: None,
        headers: vec![
            Header::from_parts(header_name.clone(), None),
            Header::from_parts(header_name.clone(), Some(Bytes::new())),
            Header::from_parts(header_name, Some(retained.clone())),
        ],
        source_owner: SourceOwner::new(Arc::clone(&source_owner)),
    });
    let header_allocation = original.headers().as_ptr();

    let prepared = prepare_engine_record(original)
        .unwrap_or_else(|_record| panic!("small header mirror should reserve"));
    let (returned, engine) = prepared.into_parts();

    assert!(Arc::ptr_eq(returned.topic_owner(), &topic));
    assert_eq!(returned.headers().as_ptr(), header_allocation);
    assert!(Arc::ptr_eq(
        &returned
            .shared_source_owner()
            .unwrap_or_else(|| panic!("source owner must remain retained")),
        &source_owner
    ));
    assert_eq!(engine.expected_topic_uuid_value(), Some([7; 16]));
    assert_eq!(
        engine.key_bytes().map(|bytes| bytes.as_ptr()),
        Some(key.as_ptr())
    );
    assert!(
        engine
            .headers()
            .iter()
            .all(|header| header.name().as_bytes().as_ptr() == header_name_bytes.as_ptr())
    );
    assert_eq!(
        engine.headers()[2].value().map(|bytes| bytes.as_ptr()),
        Some(retained.as_ptr())
    );
    drop(engine);
    assert!(!dropped.load(Ordering::Acquire));
    drop(returned);
    drop(source_owner);
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn forced_header_reserve_failure_returns_the_untouched_original() {
    let topic: Arc<str> = Arc::from("orders");
    let key = Bytes::from(vec![1, 2, 3]);
    let dropped = Arc::new(AtomicBool::new(false));
    let source_owner: Arc<dyn Send + Sync> = Arc::new(DropSentinel(Arc::clone(&dropped)));
    let original = Record::from_transfer_parts(RecordTransferParts {
        topic: Arc::clone(&topic),
        expected_topic_uuid: None,
        partition: Some(2),
        timestamp_milliseconds: Some(9),
        key: Some(key.clone()),
        value: None,
        headers: vec![Header::null("trace")],
        source_owner: SourceOwner::new(Arc::clone(&source_owner)),
    });
    let header_allocation = original.headers().as_ptr();

    let Err(returned) = prepare_engine_record_with_header_capacity(original, usize::MAX) else {
        panic!("capacity overflow must reject before cloning shared handles")
    };

    assert!(Arc::ptr_eq(returned.topic_owner(), &topic));
    assert_eq!(returned.headers().as_ptr(), header_allocation);
    assert_eq!(
        returned.key_bytes().map(|bytes| bytes.as_ptr()),
        Some(key.as_ptr())
    );
    assert!(Arc::ptr_eq(
        &returned
            .shared_source_owner()
            .unwrap_or_else(|| panic!("source owner must remain retained")),
        &source_owner
    ));
    assert!(!dropped.load(Ordering::Acquire));
}

struct DropSentinel(Arc<AtomicBool>);

impl Drop for DropSentinel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
