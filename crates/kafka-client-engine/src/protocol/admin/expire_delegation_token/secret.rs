//! Nonclone redacted ownership that zeroizes API-key 40 HMAC bytes.

use core::fmt;

use zeroize::Zeroize;

/// The sole retained owner of one expiration request's secret bytes.
#[derive(Eq, PartialEq)]
pub(super) struct ExpireDelegationTokenHmac(Vec<u8>);

impl ExpireDelegationTokenHmac {
    pub(super) fn copy_from(source: &[u8]) -> Result<Self, ()> {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(source.len()).map_err(|_| ())?;
        bytes.extend_from_slice(source);
        Ok(Self(bytes))
    }

    pub(super) fn from_decoded(source: &[u8]) -> Self {
        Self(source.to_vec())
    }

    pub(super) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(super) fn retained_capacity(&self) -> usize {
        self.0.capacity()
    }

    #[cfg(test)]
    pub(super) fn zeroize_for_test(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for ExpireDelegationTokenHmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

impl Drop for ExpireDelegationTokenHmac {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}
