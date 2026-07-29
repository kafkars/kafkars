//! Validated generated-free token facts from one API-41 response.

use core::fmt;
use std::collections::BTreeSet;

use super::super::{
    CREATE_DELEGATION_TOKEN_MAX_RENEWERS, CREATE_DELEGATION_TOKEN_MAX_REQUEST_TEXT_BYTES,
    CREATE_DELEGATION_TOKEN_MAX_TOKEN_ID_BYTES, DelegationTokenHmac, DelegationTokenPrincipal,
};

/// Maximum complete tokens retained by one description response.
pub const DESCRIBE_DELEGATION_TOKENS_MAX_TOKENS: usize = 32 * 1024;

/// One complete protocol-normalized token before deterministic ordering.
#[derive(Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokenResponse {
    owner: DelegationTokenPrincipal,
    requester: Option<DelegationTokenPrincipal>,
    renewers: Vec<DelegationTokenPrincipal>,
    issue_timestamp_ms: i64,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
    token_id: String,
    hmac: DelegationTokenHmac,
}

impl DescribeDelegationTokenResponse {
    /// Validates complete token facts, including bounded ordered renewers.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        owner: DelegationTokenPrincipal,
        requester: Option<DelegationTokenPrincipal>,
        renewers: Vec<DelegationTokenPrincipal>,
        issue_timestamp_ms: i64,
        expiry_timestamp_ms: i64,
        max_timestamp_ms: i64,
        token_id: String,
        hmac: DelegationTokenHmac,
    ) -> Result<Self, DescribeDelegationTokenResponseError> {
        validate_timestamps(issue_timestamp_ms, expiry_timestamp_ms, max_timestamp_ms)?;
        if token_id.is_empty() {
            return Err(DescribeDelegationTokenResponseError::EmptyTokenId);
        }
        if token_id.len() > CREATE_DELEGATION_TOKEN_MAX_TOKEN_ID_BYTES {
            return Err(DescribeDelegationTokenResponseError::TokenIdTooLong);
        }
        validate_principals(&owner, requester.as_ref(), &renewers)?;
        Ok(Self {
            owner,
            requester,
            renewers,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
        })
    }

    /// Returns the broker-reported token owner.
    pub const fn owner(&self) -> &DelegationTokenPrincipal {
        &self.owner
    }

    /// Returns Kafka's exact token identity.
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Consumes this fact into complete token parts.
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

/// Complete protocol-normalized response before core-owned ordering.
#[derive(Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokensResponse {
    throttle_time_ms: u32,
    tokens: Vec<DescribeDelegationTokenResponse>,
}

impl DescribeDelegationTokensResponse {
    /// Retains one bounded token collection and nonnegative throttle.
    pub fn new(
        throttle_time_ms: u32,
        tokens: Vec<DescribeDelegationTokenResponse>,
    ) -> Result<Self, DescribeDelegationTokenResponseError> {
        if tokens.len() > DESCRIBE_DELEGATION_TOKENS_MAX_TOKENS {
            return Err(DescribeDelegationTokenResponseError::TooManyTokens);
        }
        Ok(Self {
            throttle_time_ms,
            tokens,
        })
    }

    /// Consumes the response into throttle and token facts.
    pub fn into_parts(self) -> (u32, Vec<DescribeDelegationTokenResponse>) {
        (self.throttle_time_ms, self.tokens)
    }
}

fn validate_timestamps(
    issue: i64,
    expiry: i64,
    maximum: i64,
) -> Result<(), DescribeDelegationTokenResponseError> {
    if issue < 0 {
        return Err(DescribeDelegationTokenResponseError::NegativeIssueTimestamp);
    }
    if expiry < 0 {
        return Err(DescribeDelegationTokenResponseError::NegativeExpiryTimestamp);
    }
    if maximum < 0 {
        return Err(DescribeDelegationTokenResponseError::NegativeMaxTimestamp);
    }
    if expiry < issue {
        return Err(DescribeDelegationTokenResponseError::ExpiryBeforeIssue);
    }
    if maximum < expiry {
        return Err(DescribeDelegationTokenResponseError::MaxBeforeExpiry);
    }
    Ok(())
}

fn validate_principals(
    owner: &DelegationTokenPrincipal,
    requester: Option<&DelegationTokenPrincipal>,
    renewers: &[DelegationTokenPrincipal],
) -> Result<(), DescribeDelegationTokenResponseError> {
    if renewers.len() > CREATE_DELEGATION_TOKEN_MAX_RENEWERS {
        return Err(DescribeDelegationTokenResponseError::TooManyRenewers);
    }
    let mut retained = principal_bytes(owner);
    retained = retained
        .checked_add(requester.map_or(0, principal_bytes))
        .ok_or(DescribeDelegationTokenResponseError::PrincipalTextBytesExceeded)?;
    let mut identities = BTreeSet::new();
    for renewer in renewers {
        if !identities.insert(renewer) {
            return Err(DescribeDelegationTokenResponseError::DuplicateRenewer);
        }
        retained = retained
            .checked_add(principal_bytes(renewer))
            .ok_or(DescribeDelegationTokenResponseError::PrincipalTextBytesExceeded)?;
    }
    if retained > CREATE_DELEGATION_TOKEN_MAX_REQUEST_TEXT_BYTES {
        return Err(DescribeDelegationTokenResponseError::PrincipalTextBytesExceeded);
    }
    Ok(())
}

fn principal_bytes(principal: &DelegationTokenPrincipal) -> usize {
    principal.principal_type().len() + principal.principal_name().len()
}

/// Invalid complete token or response collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokenResponseError {
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
    /// One token exceeded the deterministic renewer-count bound.
    TooManyRenewers,
    /// One token repeated an exact renewer identity.
    DuplicateRenewer,
    /// Aggregate principal text exceeded the deterministic token bound.
    PrincipalTextBytesExceeded,
    /// One response exceeded the deterministic token-count bound.
    TooManyTokens,
}

impl fmt::Display for DescribeDelegationTokenResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid DescribeDelegationToken response: {self:?}"
        )
    }
}

impl std::error::Error for DescribeDelegationTokenResponseError {}
