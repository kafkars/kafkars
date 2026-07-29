//! Complete secret-safe successful result values for token description.

use core::fmt;

use zeroize::Zeroize;

use super::DescribeDelegationTokenPrincipal;

/// Unique described-token HMAC ownership with redacted diagnostics.
#[derive(Eq, PartialEq)]
pub struct DescribeDelegationTokenHmac {
    bytes: Vec<u8>,
}

impl DescribeDelegationTokenHmac {
    pub(super) const fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Borrows the exact token HMAC bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Transfers unique ownership of the token HMAC bytes.
    pub fn into_bytes(mut self) -> Vec<u8> {
        core::mem::take(&mut self.bytes)
    }
}

impl Drop for DescribeDelegationTokenHmac {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

impl fmt::Debug for DescribeDelegationTokenHmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// Complete facts for one described delegation token.
#[derive(Debug, Eq, PartialEq)]
pub struct DescribedDelegationToken {
    pub(super) owner: DescribeDelegationTokenPrincipal,
    pub(super) requester: Option<DescribeDelegationTokenPrincipal>,
    pub(super) renewers: Vec<DescribeDelegationTokenPrincipal>,
    pub(super) issue_timestamp_ms: i64,
    pub(super) expiry_timestamp_ms: i64,
    pub(super) max_timestamp_ms: i64,
    pub(super) token_id: String,
    pub(super) hmac: DescribeDelegationTokenHmac,
}

impl DescribedDelegationToken {
    /// Returns the token owner reported by Kafka.
    pub const fn owner(&self) -> &DescribeDelegationTokenPrincipal {
        &self.owner
    }

    /// Returns the requester when represented by the response version.
    pub const fn requester(&self) -> Option<&DescribeDelegationTokenPrincipal> {
        self.requester.as_ref()
    }

    /// Returns renewers in exact response order.
    pub fn renewers(&self) -> &[DescribeDelegationTokenPrincipal] {
        &self.renewers
    }

    /// Returns the token issue epoch timestamp.
    pub const fn issue_timestamp_ms(&self) -> i64 {
        self.issue_timestamp_ms
    }

    /// Returns the token expiry epoch timestamp.
    pub const fn expiry_timestamp_ms(&self) -> i64 {
        self.expiry_timestamp_ms
    }

    /// Returns the maximum token epoch timestamp.
    pub const fn max_timestamp_ms(&self) -> i64 {
        self.max_timestamp_ms
    }

    /// Returns Kafka's token identity.
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Returns the uniquely owned token HMAC.
    pub const fn hmac(&self) -> &DescribeDelegationTokenHmac {
        &self.hmac
    }

    /// Consumes this token into exact scalar parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        DescribeDelegationTokenPrincipal,
        Option<DescribeDelegationTokenPrincipal>,
        Vec<DescribeDelegationTokenPrincipal>,
        i64,
        i64,
        i64,
        String,
        DescribeDelegationTokenHmac,
    ) {
        (
            self.owner,
            self.requester,
            self.renewers,
            self.issue_timestamp_ms,
            self.expiry_timestamp_ms,
            self.max_timestamp_ms,
            self.token_id,
            self.hmac,
        )
    }
}

/// Successful deterministic token listing and Kafka throttle observation.
#[derive(Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokensResult {
    pub(super) throttle_time_ms: u32,
    pub(super) tokens: Vec<DescribedDelegationToken>,
}

impl DescribeDelegationTokensResult {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns tokens in deterministic core-owned order.
    pub fn tokens(&self) -> &[DescribedDelegationToken] {
        &self.tokens
    }

    /// Consumes the listing into throttle and complete token facts.
    pub fn into_parts(self) -> (u32, Vec<DescribedDelegationToken>) {
        (self.throttle_time_ms, self.tokens)
    }
}
