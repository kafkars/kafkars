//! Unique redacted and zeroizing ownership of one created token secret.

use core::fmt;

use zeroize::Zeroize;

use super::CreateDelegationTokenResponseError;

/// Maximum secret HMAC bytes retained by one successful token result.
pub const CREATE_DELEGATION_TOKEN_MAX_HMAC_BYTES: usize = 64 * 1024;

/// Uniquely owned token secret redacted from diagnostics.
#[derive(Eq, PartialEq)]
pub struct DelegationTokenHmac {
    bytes: Vec<u8>,
}

impl DelegationTokenHmac {
    /// Validates one nonempty bounded token secret.
    pub fn new(mut bytes: Vec<u8>) -> Result<Self, CreateDelegationTokenResponseError> {
        if bytes.is_empty() {
            bytes.zeroize();
            return Err(CreateDelegationTokenResponseError::EmptyHmac);
        }
        if bytes.len() > CREATE_DELEGATION_TOKEN_MAX_HMAC_BYTES {
            bytes.zeroize();
            return Err(CreateDelegationTokenResponseError::HmacTooLong);
        }
        Ok(Self { bytes })
    }

    /// Borrows the exact token HMAC bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Transfers unique ownership of the token HMAC bytes.
    pub fn into_bytes(mut self) -> Vec<u8> {
        core::mem::take(&mut self.bytes)
    }

    #[cfg(test)]
    pub(super) fn zeroize_for_test(&mut self) {
        self.bytes.zeroize();
    }
}

impl Drop for DelegationTokenHmac {
    fn drop(&mut self) {
        self.bytes.zeroize();
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
