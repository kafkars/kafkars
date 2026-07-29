//! Zeroizing and diagnostic-safe ownership of delegation-token HMAC bytes.

use core::fmt;

use zeroize::Zeroize;

/// Secret HMAC bytes with redacted diagnostics and zeroized final release.
#[derive(Eq, PartialEq)]
pub(crate) struct DelegationTokenHmac(Vec<u8>);

impl DelegationTokenHmac {
    pub(super) const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn into_bytes(mut self) -> Vec<u8> {
        core::mem::take(&mut self.0)
    }

    pub(super) fn retained_capacity(&self) -> usize {
        self.0.capacity()
    }
}

impl fmt::Debug for DelegationTokenHmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

impl Drop for DelegationTokenHmac {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}
