//! Unique redacted and zeroizing ownership of one token-expiration secret.

use core::fmt;

use zeroize::Zeroize;

use super::ExpireDelegationTokenPlanError;

/// Maximum secret HMAC bytes retained by one token-expiration request.
pub const EXPIRE_DELEGATION_TOKEN_MAX_HMAC_BYTES: usize = 64 * 1024;

/// Uniquely owned request HMAC redacted from diagnostics.
#[derive(Eq, PartialEq)]
pub struct ExpireDelegationTokenHmac {
    bytes: Vec<u8>,
}

impl ExpireDelegationTokenHmac {
    /// Validates one nonempty bounded token secret.
    pub fn new(mut bytes: Vec<u8>) -> Result<Self, ExpireDelegationTokenPlanError> {
        if bytes.is_empty() {
            bytes.zeroize();
            return Err(ExpireDelegationTokenPlanError::EmptyHmac);
        }
        if bytes.len() > EXPIRE_DELEGATION_TOKEN_MAX_HMAC_BYTES {
            bytes.zeroize();
            return Err(ExpireDelegationTokenPlanError::HmacTooLong);
        }
        Ok(Self { bytes })
    }

    /// Borrows the exact token HMAC bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the number of uniquely retained secret bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Reports whether the retained secret is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Transfers unique ownership of the secret bytes.
    pub fn into_bytes(mut self) -> Vec<u8> {
        core::mem::take(&mut self.bytes)
    }

    fn zeroize(&mut self) {
        self.bytes.as_mut_slice().zeroize();
    }

    #[cfg(test)]
    pub(super) fn zeroize_for_test(&mut self) {
        self.zeroize();
    }
}

impl Drop for ExpireDelegationTokenHmac {
    fn drop(&mut self) {
        self.zeroize();
    }
}

impl fmt::Debug for ExpireDelegationTokenHmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExpireDelegationTokenHmac")
            .field("bytes", &"<redacted>")
            .finish()
    }
}
