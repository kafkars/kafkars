//! Validated shared producer header names and opaque source-owner transfer.

use std::sync::Arc;

use bytes::Bytes;

use crate::producer::record::{ProducerHeader as StoredProducerHeader, ProducerSourceOwner};

/// UTF-8-validated shared name bytes at the public engine boundary.
#[derive(Debug, Eq, PartialEq)]
struct ProducerHeaderName(Bytes);

impl ProducerHeaderName {
    fn new(name: String) -> Self {
        Self(Bytes::from(name))
    }

    fn try_from_shared(name: Bytes) -> Result<Self, std::str::Utf8Error> {
        std::str::from_utf8(&name)?;
        Ok(Self(name))
    }

    fn shared_bytes(&self) -> Bytes {
        self.0.clone()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0)
            .unwrap_or_else(|_error| unreachable!("producer header name was validated"))
    }

    fn into_bytes(self) -> Bytes {
        self.0
    }
}

/// One ordered Kafka header with a non-null name and nullable bytes.
#[derive(Debug, Eq, PartialEq)]
pub struct ProducerHeader {
    name: ProducerHeaderName,
    value: Option<Bytes>,
    source_owner: ProducerSourceOwner,
}

impl ProducerHeader {
    /// Creates a header with a non-null value.
    pub fn new(name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            name: ProducerHeaderName::new(name.into()),
            value: Some(value.into()),
            source_owner: ProducerSourceOwner::none(),
        }
    }

    /// Creates a header with a null value.
    pub fn null(name: impl Into<String>) -> Self {
        Self {
            name: ProducerHeaderName::new(name.into()),
            value: None,
            source_owner: ProducerSourceOwner::none(),
        }
    }

    /// Retains shared name bytes after validating UTF-8 without allocating a name.
    pub fn try_from_shared_name(
        name: Bytes,
        value: Option<Bytes>,
    ) -> Result<Self, std::str::Utf8Error> {
        Ok(Self {
            name: ProducerHeaderName::try_from_shared(name)?,
            value,
            source_owner: ProducerSourceOwner::none(),
        })
    }

    /// Attaches an opaque source lease to this shared header name.
    #[doc(hidden)]
    pub fn retain_source_owner(mut self, owner: Arc<dyn Send + Sync>) -> Self {
        self.source_owner = ProducerSourceOwner::new(owner);
        self
    }

    /// Returns the header name.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns the nullable header bytes.
    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    /// Transfers shared validated name and nullable value ownership.
    #[doc(hidden)]
    pub fn into_shared_parts(self) -> (Bytes, Option<Bytes>, Option<Arc<dyn Send + Sync>>) {
        (
            self.name.into_bytes(),
            self.value,
            self.source_owner.into_inner(),
        )
    }

    pub(in crate::producer::boundary) fn shared_name_bytes(&self) -> Bytes {
        self.name.shared_bytes()
    }

    pub(in crate::producer::boundary) fn name_len(&self) -> usize {
        self.name.len()
    }

    pub(in crate::producer::boundary) fn shared_value(&self) -> Option<Bytes> {
        self.value.clone()
    }

    pub(super) fn into_stored(self) -> StoredProducerHeader {
        StoredProducerHeader::from_shared(self.name.into_bytes(), self.value, self.source_owner)
    }

    pub(super) fn from_stored(header: StoredProducerHeader) -> Self {
        let (name, value, source_owner) = header.into_parts();
        Self {
            name: ProducerHeaderName::try_from_shared(name)
                .unwrap_or_else(|_error| unreachable!("stored header name was validated")),
            value,
            source_owner,
        }
    }
}
