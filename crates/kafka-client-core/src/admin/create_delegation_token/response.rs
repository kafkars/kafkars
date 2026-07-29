//! Validated protocol-normalized successful API-38 response facts.

use core::fmt;

use super::{DelegationTokenHmac, DelegationTokenPrincipal};

/// Maximum UTF-8 bytes retained for one token identity.
pub const CREATE_DELEGATION_TOKEN_MAX_TOKEN_ID_BYTES: usize = i16::MAX as usize;

/// Successful broker fields before request-owned renewers join the terminal.
#[derive(Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenResponse {
    throttle_time_ms: u32,
    owner: DelegationTokenPrincipal,
    requester: Option<DelegationTokenPrincipal>,
    issue_timestamp_ms: i64,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
    token_id: String,
    hmac: DelegationTokenHmac,
}

impl CreateDelegationTokenResponse {
    /// Validates complete successful response facts before machine input.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        throttle_time_ms: u32,
        owner: DelegationTokenPrincipal,
        requester: Option<DelegationTokenPrincipal>,
        issue_timestamp_ms: i64,
        expiry_timestamp_ms: i64,
        max_timestamp_ms: i64,
        token_id: String,
        hmac: DelegationTokenHmac,
    ) -> Result<Self, CreateDelegationTokenResponseError> {
        if issue_timestamp_ms < 0 {
            return Err(CreateDelegationTokenResponseError::NegativeIssueTimestamp);
        }
        if expiry_timestamp_ms < 0 {
            return Err(CreateDelegationTokenResponseError::NegativeExpiryTimestamp);
        }
        if max_timestamp_ms < 0 {
            return Err(CreateDelegationTokenResponseError::NegativeMaxTimestamp);
        }
        if expiry_timestamp_ms < issue_timestamp_ms {
            return Err(CreateDelegationTokenResponseError::ExpiryBeforeIssue);
        }
        if max_timestamp_ms < expiry_timestamp_ms {
            return Err(CreateDelegationTokenResponseError::MaxBeforeExpiry);
        }
        if token_id.is_empty() {
            return Err(CreateDelegationTokenResponseError::EmptyTokenId);
        }
        if token_id.len() > CREATE_DELEGATION_TOKEN_MAX_TOKEN_ID_BYTES {
            return Err(CreateDelegationTokenResponseError::TokenIdTooLong);
        }
        Ok(Self {
            throttle_time_ms,
            owner,
            requester,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
        })
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the broker-reported token owner.
    pub const fn owner(&self) -> &DelegationTokenPrincipal {
        &self.owner
    }

    /// Consumes the response into adapter-independent scalar parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        u32,
        DelegationTokenPrincipal,
        Option<DelegationTokenPrincipal>,
        i64,
        i64,
        i64,
        String,
        DelegationTokenHmac,
    ) {
        (
            self.throttle_time_ms,
            self.owner,
            self.requester,
            self.issue_timestamp_ms,
            self.expiry_timestamp_ms,
            self.max_timestamp_ms,
            self.token_id,
            self.hmac,
        )
    }
}

/// Invalid protocol-normalized successful response facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenResponseError {
    /// The broker reported a negative issue timestamp.
    NegativeIssueTimestamp,
    /// The broker reported a negative expiry timestamp.
    NegativeExpiryTimestamp,
    /// The broker reported a negative maximum timestamp.
    NegativeMaxTimestamp,
    /// Expiry preceded token issuance.
    ExpiryBeforeIssue,
    /// Maximum lifetime ended before ordinary expiry.
    MaxBeforeExpiry,
    /// Successful token identities cannot be empty.
    EmptyTokenId,
    /// The token identity exceeded Kafka's string domain.
    TokenIdTooLong,
    /// Successful token secrets cannot be empty.
    EmptyHmac,
    /// The token secret exceeded the deterministic retained bound.
    HmacTooLong,
}

impl fmt::Display for CreateDelegationTokenResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid CreateDelegationToken response: {self:?}"
        )
    }
}

impl std::error::Error for CreateDelegationTokenResponseError {}
