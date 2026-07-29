//! Allocation-free version, scalar, identity, token, and HMAC validation.

use kafka_wire::CreateDelegationTokenResponse;

use super::retention::{
    MAX_HMAC_BYTES, MAX_PRINCIPAL_NAME_BYTES, MAX_PRINCIPAL_TYPE_BYTES, MAX_TOKEN_ID_BYTES,
};

pub(super) const MIN_VERSION: i16 = 1;
pub(super) const MAX_VERSION: i16 = 3;

/// Incompatible, malformed, allocation-failed, or over-capacity response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateDelegationTokenResponseFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
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
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

pub(super) fn validate_success(
    selected_version: i16,
    response: &CreateDelegationTokenResponse,
) -> Result<(), CreateDelegationTokenResponseFailure> {
    validate_principal(
        "owner",
        response.principal_type.as_str(),
        response.principal_name.as_str(),
    )?;
    if selected_version >= 3 {
        validate_principal(
            "requester",
            response.token_requester_principal_type.as_str(),
            response.token_requester_principal_name.as_str(),
        )?;
    }
    if response.issue_timestamp_ms < 0 {
        return Err(
            CreateDelegationTokenResponseFailure::InvalidIssueTimestamp {
                actual: response.issue_timestamp_ms,
            },
        );
    }
    if response.expiry_timestamp_ms < response.issue_timestamp_ms {
        return Err(
            CreateDelegationTokenResponseFailure::InvalidExpiryTimestamp {
                issue: response.issue_timestamp_ms,
                expiry: response.expiry_timestamp_ms,
            },
        );
    }
    if response.max_timestamp_ms < response.expiry_timestamp_ms {
        return Err(CreateDelegationTokenResponseFailure::InvalidMaxTimestamp {
            expiry: response.expiry_timestamp_ms,
            max: response.max_timestamp_ms,
        });
    }
    if response.token_id.is_empty() {
        return Err(CreateDelegationTokenResponseFailure::EmptyTokenId);
    }
    if response.token_id.len() > MAX_TOKEN_ID_BYTES {
        return Err(CreateDelegationTokenResponseFailure::TokenIdTooLong {
            actual: response.token_id.len(),
            max: MAX_TOKEN_ID_BYTES,
        });
    }
    if response.hmac.is_empty() {
        return Err(CreateDelegationTokenResponseFailure::EmptyHmac);
    }
    if response.hmac.len() > MAX_HMAC_BYTES {
        return Err(CreateDelegationTokenResponseFailure::HmacTooLong {
            actual: response.hmac.len(),
            max: MAX_HMAC_BYTES,
        });
    }
    Ok(())
}

fn validate_principal(
    field: &'static str,
    principal_type: &str,
    principal_name: &str,
) -> Result<(), CreateDelegationTokenResponseFailure> {
    if principal_type.is_empty() {
        return Err(CreateDelegationTokenResponseFailure::EmptyPrincipalType { field });
    }
    if principal_type.len() > MAX_PRINCIPAL_TYPE_BYTES {
        return Err(CreateDelegationTokenResponseFailure::PrincipalTypeTooLong {
            field,
            actual: principal_type.len(),
            max: MAX_PRINCIPAL_TYPE_BYTES,
        });
    }
    if principal_name.is_empty() {
        return Err(CreateDelegationTokenResponseFailure::EmptyPrincipalName { field });
    }
    if principal_name.len() > MAX_PRINCIPAL_NAME_BYTES {
        return Err(CreateDelegationTokenResponseFailure::PrincipalNameTooLong {
            field,
            actual: principal_name.len(),
            max: MAX_PRINCIPAL_NAME_BYTES,
        });
    }
    Ok(())
}
