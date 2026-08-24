//! Validated shared UTF-8 Kafka header names owned by the Rust facade.

use std::{borrow::Borrow, fmt, hash::Hash, sync::Arc};

use bytes::Bytes;

/// Opaque upstream lease retained by every derived public byte owner.
#[derive(Clone)]
pub(crate) struct SourceOwner(Option<Arc<dyn Send + Sync>>);

impl SourceOwner {
    pub(crate) const fn none() -> Self {
        Self(None)
    }

    pub(crate) fn new(owner: Arc<dyn Send + Sync>) -> Self {
        Self(Some(owner))
    }

    pub(crate) const fn from_optional(owner: Option<Arc<dyn Send + Sync>>) -> Self {
        Self(owner)
    }

    pub(crate) fn into_arc(self) -> Option<Arc<dyn Send + Sync>> {
        self.0
    }
}

impl fmt::Debug for SourceOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceOwner")
            .field("retained", &self.0.is_some())
            .finish()
    }
}

impl PartialEq for SourceOwner {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SourceOwner {}

/// A non-null Kafka header name constructed only after UTF-8 validation.
///
/// Cloning this value shares the exact name allocation. Names obtained from
/// an owned consumer record also retain that record's delivery lease.
#[derive(Clone, Eq, PartialEq)]
pub struct HeaderName {
    bytes: Bytes,
    source_owner: SourceOwner,
}

impl HeaderName {
    /// Validates and retains a shared byte owner without copying its contents.
    pub fn try_from_bytes(bytes: Bytes) -> Result<Self, std::str::Utf8Error> {
        std::str::from_utf8(&bytes)?;
        Ok(Self {
            bytes,
            source_owner: SourceOwner::none(),
        })
    }

    /// Returns the validated UTF-8 name.
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes)
            .unwrap_or_else(|_error| unreachable!("header name was validated at construction"))
    }

    /// Returns the name bytes without exposing a clonable byte owner.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn from_shared(bytes: Bytes, source_owner: SourceOwner) -> Self {
        debug_assert!(std::str::from_utf8(&bytes).is_ok());
        Self {
            bytes,
            source_owner,
        }
    }

    pub(crate) fn into_shared_parts(self) -> (Bytes, SourceOwner) {
        (self.bytes, self.source_owner)
    }
}

impl From<String> for HeaderName {
    fn from(name: String) -> Self {
        Self {
            bytes: Bytes::from(name),
            source_owner: SourceOwner::none(),
        }
    }
}

impl From<&str> for HeaderName {
    fn from(name: &str) -> Self {
        Self::from(name.to_owned())
    }
}

impl TryFrom<Bytes> for HeaderName {
    type Error = std::str::Utf8Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl AsRef<str> for HeaderName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for HeaderName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Hash for HeaderName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl fmt::Debug for HeaderName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("HeaderName")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for HeaderName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
