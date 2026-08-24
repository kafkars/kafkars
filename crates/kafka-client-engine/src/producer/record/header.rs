//! Shared header-name storage, conservative control accounting, and source ownership.

use std::{
    mem::size_of,
    sync::{Arc, atomic::AtomicUsize},
};

use bytes::Bytes;

use crate::producer::{ProducerStoreError, materialization::MaterializationHeader};

#[repr(C)]
struct BytesOwnerControlLayout {
    _reference_count: AtomicUsize,
    _owner: String,
}

pub(in crate::producer) const HEADER_BYTES_OWNER_CONTROL_BYTES: usize =
    size_of::<BytesOwnerControlLayout>();
// One inline vector element plus at most one facade-created Bytes name owner.
pub(in crate::producer) const HEADER_CONTROL_BYTES: usize =
    size_of::<ProducerHeader>() + HEADER_BYTES_OWNER_CONTROL_BYTES;

/// Opaque upstream owner retained until producer byte admission commits.
pub(in crate::producer) struct ProducerSourceOwner(Option<Arc<dyn Send + Sync>>);

impl ProducerSourceOwner {
    pub(in crate::producer) const fn none() -> Self {
        Self(None)
    }

    pub(in crate::producer) fn new(owner: Arc<dyn Send + Sync>) -> Self {
        Self(Some(owner))
    }

    pub(in crate::producer) fn into_inner(self) -> Option<Arc<dyn Send + Sync>> {
        self.0
    }

    pub(super) fn release(&mut self) {
        self.0 = None;
    }
}

impl std::fmt::Debug for ProducerSourceOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerSourceOwner")
            .field("retained", &self.0.is_some())
            .finish()
    }
}

impl PartialEq for ProducerSourceOwner {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ProducerSourceOwner {}

#[derive(Debug, Eq, PartialEq)]
struct ValidatedHeaderName {
    bytes: Bytes,
}

impl ValidatedHeaderName {
    fn new(text: String) -> Self {
        Self {
            bytes: Bytes::from(text),
        }
    }

    fn from_shared(bytes: Bytes) -> Self {
        debug_assert!(std::str::from_utf8(&bytes).is_ok());
        Self { bytes }
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn shared_bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

/// One ordered Kafka header with a non-null name and nullable value.
#[derive(Debug, Eq, PartialEq)]
pub(in crate::producer) struct ProducerHeader {
    name: ValidatedHeaderName,
    value: Option<Bytes>,
    source_owner: ProducerSourceOwner,
}

impl ProducerHeader {
    /// Captures a UTF-8-validated header name as shared immutable bytes.
    pub(crate) fn new(name: String, value: Option<Bytes>) -> Self {
        Self {
            name: ValidatedHeaderName::new(name),
            value,
            source_owner: ProducerSourceOwner::none(),
        }
    }

    /// Retains already-validated shared UTF-8 name bytes without allocation.
    pub(in crate::producer) fn from_shared(
        name: Bytes,
        value: Option<Bytes>,
        source_owner: ProducerSourceOwner,
    ) -> Self {
        Self {
            name: ValidatedHeaderName::from_shared(name),
            value,
            source_owner,
        }
    }

    pub(in crate::producer) fn retained_bytes(&self) -> Result<usize, ProducerStoreError> {
        self.name
            .len()
            .checked_add(self.value.as_ref().map_or(0, Bytes::len))
            .ok_or(ProducerStoreError::RetainedSizeOverflow)
    }

    pub(in crate::producer) fn materialization_view(&self) -> MaterializationHeader {
        MaterializationHeader::new(self.name.shared_bytes(), self.value.clone())
    }

    pub(in crate::producer) fn release_source_owner(&mut self) {
        self.source_owner.release();
    }

    pub(in crate::producer) fn into_parts(self) -> (Bytes, Option<Bytes>, ProducerSourceOwner) {
        (self.name.into_bytes(), self.value, self.source_owner)
    }
}
