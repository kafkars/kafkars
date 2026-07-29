//! Complete created token facts and nonnegative throttle observation.

use super::{DelegationTokenHmac, DelegationTokenPrincipal};

/// Complete created token facts, including caller-ordered renewers.
#[derive(Debug, Eq, PartialEq)]
pub struct DelegationToken {
    owner: DelegationTokenPrincipal,
    requester: Option<DelegationTokenPrincipal>,
    renewers: Vec<DelegationTokenPrincipal>,
    issue_timestamp_ms: i64,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
    token_id: String,
    hmac: DelegationTokenHmac,
}

impl DelegationToken {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        owner: DelegationTokenPrincipal,
        requester: Option<DelegationTokenPrincipal>,
        renewers: Vec<DelegationTokenPrincipal>,
        issue_timestamp_ms: i64,
        expiry_timestamp_ms: i64,
        max_timestamp_ms: i64,
        token_id: String,
        hmac: DelegationTokenHmac,
    ) -> Self {
        Self {
            owner,
            requester,
            renewers,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
        }
    }

    /// Returns the token owner reported by Kafka.
    pub const fn owner(&self) -> &DelegationTokenPrincipal {
        &self.owner
    }

    /// Returns the requester when represented by the response version.
    pub const fn requester(&self) -> Option<&DelegationTokenPrincipal> {
        self.requester.as_ref()
    }

    /// Returns renewers in exact caller order.
    pub fn renewers(&self) -> &[DelegationTokenPrincipal] {
        &self.renewers
    }

    /// Returns the nonnegative token issue epoch timestamp.
    pub const fn issue_timestamp_ms(&self) -> i64 {
        self.issue_timestamp_ms
    }

    /// Returns the nonnegative token expiry epoch timestamp.
    pub const fn expiry_timestamp_ms(&self) -> i64 {
        self.expiry_timestamp_ms
    }

    /// Returns the nonnegative maximum token epoch timestamp.
    pub const fn max_timestamp_ms(&self) -> i64 {
        self.max_timestamp_ms
    }

    /// Returns Kafka's token identity.
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Returns the uniquely retained token HMAC.
    pub const fn hmac(&self) -> &DelegationTokenHmac {
        &self.hmac
    }

    /// Consumes the token into adapter-owned scalar parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        DelegationTokenPrincipal,
        Option<DelegationTokenPrincipal>,
        Vec<DelegationTokenPrincipal>,
        i64,
        i64,
        i64,
        String,
        DelegationTokenHmac,
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
pub struct CreateDelegationTokenSuccess {
    throttle_time_ms: u32,
    token: DelegationToken,
}

impl CreateDelegationTokenSuccess {
    pub(crate) const fn new(throttle_time_ms: u32, token: DelegationToken) -> Self {
        Self {
            throttle_time_ms,
            token,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the complete created token.
    pub const fn token(&self) -> &DelegationToken {
        &self.token
    }

    /// Consumes success into throttle and complete token facts.
    pub fn into_parts(self) -> (u32, DelegationToken) {
        (self.throttle_time_ms, self.token)
    }
}
