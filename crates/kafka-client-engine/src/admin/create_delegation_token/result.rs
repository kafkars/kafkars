//! Complete secret-safe successful result values for token creation.

use core::fmt;

use zeroize::Zeroize;

use super::CreateDelegationTokenPrincipal;

/// Unique token HMAC ownership with redacted diagnostics and zeroized release.
#[derive(Eq, PartialEq)]
pub struct CreateDelegationTokenHmac {
    bytes: Vec<u8>,
}

impl CreateDelegationTokenHmac {
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

impl Drop for CreateDelegationTokenHmac {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

impl fmt::Debug for CreateDelegationTokenHmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// Complete created token facts, including caller-ordered renewers.
#[derive(Debug, Eq, PartialEq)]
pub struct CreatedDelegationToken {
    pub(super) owner: CreateDelegationTokenPrincipal,
    pub(super) requester: Option<CreateDelegationTokenPrincipal>,
    pub(super) renewers: Vec<CreateDelegationTokenPrincipal>,
    pub(super) issue_timestamp_ms: i64,
    pub(super) expiry_timestamp_ms: i64,
    pub(super) max_timestamp_ms: i64,
    pub(super) token_id: String,
    pub(super) hmac: CreateDelegationTokenHmac,
}

impl CreatedDelegationToken {
    /// Returns the token owner reported by Kafka.
    pub const fn owner(&self) -> &CreateDelegationTokenPrincipal {
        &self.owner
    }

    /// Returns the requester when represented by the response version.
    pub const fn requester(&self) -> Option<&CreateDelegationTokenPrincipal> {
        self.requester.as_ref()
    }

    /// Returns renewers in exact caller order.
    pub fn renewers(&self) -> &[CreateDelegationTokenPrincipal] {
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
    pub const fn hmac(&self) -> &CreateDelegationTokenHmac {
        &self.hmac
    }

    /// Consumes this token into exact scalar parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        CreateDelegationTokenPrincipal,
        Option<CreateDelegationTokenPrincipal>,
        Vec<CreateDelegationTokenPrincipal>,
        i64,
        i64,
        i64,
        String,
        CreateDelegationTokenHmac,
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

/// Successful token creation and Kafka's throttle observation.
#[derive(Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenResult {
    pub(super) throttle_time_ms: u32,
    pub(super) token: CreatedDelegationToken,
}

impl CreateDelegationTokenResult {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the complete created token.
    pub const fn token(&self) -> &CreatedDelegationToken {
        &self.token
    }

    /// Consumes success into throttle and complete token facts.
    pub fn into_parts(self) -> (u32, CreatedDelegationToken) {
        (self.throttle_time_ms, self.token)
    }
}
