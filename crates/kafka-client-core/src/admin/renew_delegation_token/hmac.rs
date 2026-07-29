//! Unique redacted and zeroizing ownership of one token-renewal secret.

use core::fmt;

use zeroize::Zeroize;

use super::RenewDelegationTokenPlanError;

/// Maximum secret HMAC bytes retained by one token-renewal request.
pub const RENEW_DELEGATION_TOKEN_MAX_HMAC_BYTES: usize = 64 * 1024;

/// Uniquely owned request HMAC redacted from diagnostics.
#[derive(Eq, PartialEq)]
pub struct RenewDelegationTokenHmac {
    bytes: Vec<u8>,
}

impl RenewDelegationTokenHmac {
    /// Validates one nonempty bounded token secret.
    pub fn new(mut bytes: Vec<u8>) -> Result<Self, RenewDelegationTokenPlanError> {
        if bytes.is_empty() {
            bytes.zeroize();
            return Err(RenewDelegationTokenPlanError::EmptyHmac);
        }
        if bytes.len() > RENEW_DELEGATION_TOKEN_MAX_HMAC_BYTES {
            bytes.zeroize();
            return Err(RenewDelegationTokenPlanError::HmacTooLong);
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

impl Drop for RenewDelegationTokenHmac {
    fn drop(&mut self) {
        self.zeroize();
    }
}

impl fmt::Debug for RenewDelegationTokenHmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RenewDelegationTokenHmac")
            .field("bytes", &"<redacted>")
            .finish()
    }
}
