//! Complete created delegation-token facts and unique secret ownership.

use super::{DelegationTokenHmac, DelegationTokenPrincipal};

/// One complete broker-created delegation token.
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

    /// Returns the requester when represented by the selected response version.
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

    /// Returns Kafka's exact token identity.
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Returns the uniquely retained token secret.
    pub const fn hmac(&self) -> &DelegationTokenHmac {
        &self.hmac
    }

    /// Consumes the token into stable scalar parts and the unique secret.
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
