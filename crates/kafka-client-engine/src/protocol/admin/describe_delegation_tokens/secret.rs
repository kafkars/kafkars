//! Zeroizing and diagnostic-safe ownership of one described token HMAC.

use core::fmt;

use zeroize::Zeroize;

/// Secret HMAC bytes copied out of one generated token response.
#[derive(Eq, PartialEq)]
pub(crate) struct DescribeDelegationTokenHmac(Vec<u8>);

impl DescribeDelegationTokenHmac {
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

impl fmt::Debug for DescribeDelegationTokenHmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

impl Drop for DescribeDelegationTokenHmac {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}
