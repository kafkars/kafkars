//! Allocation-free version, selection, scalar, identity, and secret validation.

use kafka_wire::DescribeDelegationTokenResponse;

use super::{
    DescribeDelegationTokensRequestRef,
    retention::MAX_TOKENS,
    shape::{validate_selection, validate_token},
};

pub(super) const MIN_VERSION: i16 = 1;
pub(super) const MAX_VERSION: i16 = 3;

/// Incompatible, malformed, allocation-failed, or over-capacity response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeDelegationTokensResponseFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    TokensWithTopLevelError {
        actual: usize,
    },
    TooManyTokens {
        actual: usize,
        max: usize,
    },
    EmptyOwnerSelection,
    TooManyRequestedOwners {
        actual: usize,
        max: usize,
    },
    DuplicateRequestedOwner,
    UnexpectedOwner,
    DuplicateToken,
    EmptyPrincipalType {
        field: &'static str,
    },
    PrincipalTypeTooLong {
        field: &'static str,
        actual: usize,
        max: usize,
    },
    EmptyPrincipalName {
        field: &'static str,
    },
    PrincipalNameTooLong {
        field: &'static str,
        actual: usize,
        max: usize,
    },
    UnexpectedRequesterBeforeV3,
    InvalidIssueTimestamp {
        actual: i64,
    },
    InvalidExpiryTimestamp {
        issue: i64,
        expiry: i64,
    },
    InvalidMaxTimestamp {
        expiry: i64,
        max: i64,
    },
    EmptyTokenId,
    TokenIdTooLong {
        actual: usize,
        max: usize,
    },
    EmptyHmac,
    HmacTooLong {
        actual: usize,
        max: usize,
    },
    TooManyRenewers {
        actual: usize,
        max: usize,
    },
    DuplicateRenewer,
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

pub(super) fn validate_response(
    selected_version: i16,
    request: DescribeDelegationTokensRequestRef<'_>,
    response: &DescribeDelegationTokenResponse,
) -> Result<(), DescribeDelegationTokensResponseFailure> {
    if !(MIN_VERSION..=MAX_VERSION).contains(&selected_version) {
        return Err(
            DescribeDelegationTokensResponseFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    if response.throttle_time_ms < 0 {
        return Err(
            DescribeDelegationTokensResponseFailure::NegativeThrottleTime {
                actual: response.throttle_time_ms,
            },
        );
    }
    validate_selection(request)?;
    if response.error_code != 0 {
        return response.tokens.is_empty().then_some(()).ok_or(
            DescribeDelegationTokensResponseFailure::TokensWithTopLevelError {
                actual: response.tokens.len(),
            },
        );
    }
    if response.tokens.len() > MAX_TOKENS {
        return Err(DescribeDelegationTokensResponseFailure::TooManyTokens {
            actual: response.tokens.len(),
            max: MAX_TOKENS,
        });
    }
    for (index, token) in response.tokens.iter().enumerate() {
        validate_token(selected_version, request, token)?;
        if response.tokens[..index].iter().any(|prior| {
            prior.principal_type == token.principal_type
                && prior.principal_name == token.principal_name
                && prior.token_id == token.token_id
        }) {
            return Err(DescribeDelegationTokensResponseFailure::DuplicateToken);
        }
    }
    Ok(())
}
