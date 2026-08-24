//! Shared header-name validation, identity, and source-owner scenarios.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;

use super::header_name::{HeaderName, SourceOwner};

#[test]
fn shared_utf8_name_preserves_pointer_identity_without_name_allocation() {
    let bytes = Bytes::from("trace-�-é".to_owned());
    let pointer = bytes.as_ptr();
    let name = HeaderName::try_from_bytes(bytes)
        .unwrap_or_else(|error| panic!("valid header name: {error}"));
    let clone = name.clone();

    assert_eq!(name.as_str(), "trace-�-é");
    assert_eq!(name.as_bytes().as_ptr(), pointer);
    assert_eq!(clone.as_bytes().as_ptr(), pointer);
}

#[test]
fn non_utf8_name_is_rejected_without_changing_policy() {
    assert!(HeaderName::try_from_bytes(Bytes::from_static(b"\xff")).is_err());
}

#[test]
fn cloned_name_retains_its_opaque_source_owner() {
    let dropped = Arc::new(AtomicBool::new(false));
    let sentinel: Arc<dyn Send + Sync> = Arc::new(DropSentinel(Arc::clone(&dropped)));
    let name = HeaderName::from_shared(Bytes::from_static(b"trace"), SourceOwner::new(sentinel));
    let clone = name.clone();

    drop(name);
    assert!(!dropped.load(Ordering::Acquire));
    drop(clone);
    assert!(dropped.load(Ordering::Acquire));
}

struct DropSentinel(Arc<AtomicBool>);

impl Drop for DropSentinel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
