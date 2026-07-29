//! Uniquely owned, redacted, zeroizing delegation-token secret.

use core::fmt;

use zeroize::Zeroize;

const MAX_HMAC_BYTES: usize = 64 * 1024;

/// Secret HMAC returned for one created delegation token.
///
/// This value is deliberately non-cloneable. Its bytes are redacted from
/// diagnostics and zeroized when the final unique owner is dropped.
#[derive(Eq, PartialEq)]
pub struct DelegationTokenHmac {
    bytes: Vec<u8>,
}

impl DelegationTokenHmac {
    pub(crate) const fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Validates uniquely owned HMAC bytes reconstructed from durable storage.
    ///
    /// Empty values and values larger than 64 KiB are rejected. Rejected bytes
    /// are zeroized before their allocation is released.
    pub fn from_bytes(mut bytes: Vec<u8>) -> Result<Self, DelegationTokenHmacError> {
        let error = if bytes.is_empty() {
            Some(DelegationTokenHmacError::Empty)
        } else if bytes.len() > MAX_HMAC_BYTES {
            Some(DelegationTokenHmacError::TooLong {
                actual: bytes.len(),
                maximum: MAX_HMAC_BYTES,
            })
        } else {
            None
        };
        if let Some(error) = error {
            bytes.zeroize();
            return Err(error);
        }
        Ok(Self { bytes })
    }

    /// Borrows the exact token HMAC bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the number of retained secret bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Reports whether Kafka returned an empty secret.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Transfers unique ownership of the secret bytes to the caller.
    pub fn into_bytes(mut self) -> Vec<u8> {
        core::mem::take(&mut self.bytes)
    }

    fn zeroize(&mut self) {
        self.bytes.zeroize();
    }

    #[cfg(test)]
    pub(super) fn zeroize_for_test(&mut self) {
        self.zeroize();
    }
}

impl Drop for DelegationTokenHmac {
    fn drop(&mut self) {
        self.zeroize();
    }
}

impl fmt::Debug for DelegationTokenHmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DelegationTokenHmac")
            .field("bytes", &"<redacted>")
            .finish()
    }
}

/// Local validation failure for reconstructed delegation-token HMAC bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DelegationTokenHmacError {
    /// Kafka token HMACs cannot be empty.
    Empty,
    /// The secret exceeded the bounded 64-KiB ownership envelope.
    TooLong {
        /// Number of bytes supplied by the caller.
        actual: usize,
        /// Maximum accepted number of bytes.
        maximum: usize,
    },
}

impl fmt::Display for DelegationTokenHmacError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("delegation-token HMAC must not be empty"),
            Self::TooLong { actual, maximum } => write!(
                formatter,
                "delegation-token HMAC is {actual} bytes; maximum is {maximum} bytes"
            ),
        }
    }
}

impl std::error::Error for DelegationTokenHmacError {}
