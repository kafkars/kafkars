//! Validate-first API-key 40 secret ownership and period adaptation.

use super::{
    ExpireDelegationTokenRequestRef, PreparedExpireDelegationTokenRequest,
    retention::{EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, MAX_HMAC_BYTES, request_peak_charge},
    secret::ExpireDelegationTokenHmac,
};

/// Invalid intent, allocation failure, or insufficient retained capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpireDelegationTokenRequestFailure {
    EmptyHmac,
    HmacTooLong {
        actual: usize,
        max: usize,
    },
    InvalidExpiryTimePeriod {
        actual: i64,
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

/// Validates every scalar and charge before copying the secret exactly once.
pub(crate) fn expire_delegation_token_request(
    source: ExpireDelegationTokenRequestRef<'_>,
    retained_limit: usize,
) -> Result<PreparedExpireDelegationTokenRequest, ExpireDelegationTokenRequestFailure> {
    validate(source)?;
    let effective_limit = retained_limit.min(EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES);
    let required = request_peak_charge(source).unwrap_or(usize::MAX);
    ensure_limit(required, effective_limit)?;
    let hmac = ExpireDelegationTokenHmac::copy_from(source.hmac()).map_err(|()| {
        ExpireDelegationTokenRequestFailure::Allocation {
            field: "hmac",
            requested: source.hmac().len(),
        }
    })?;
    let prepared = PreparedExpireDelegationTokenRequest::new(hmac, source.expiry_time_period_ms());
    ensure_limit(prepared.retained_heap_bytes(), effective_limit)?;
    Ok(prepared)
}

fn validate(
    source: ExpireDelegationTokenRequestRef<'_>,
) -> Result<(), ExpireDelegationTokenRequestFailure> {
    if source.hmac().is_empty() {
        return Err(ExpireDelegationTokenRequestFailure::EmptyHmac);
    }
    if source.hmac().len() > MAX_HMAC_BYTES {
        return Err(ExpireDelegationTokenRequestFailure::HmacTooLong {
            actual: source.hmac().len(),
            max: MAX_HMAC_BYTES,
        });
    }
    if !source.is_immediate() && source.expiry_time_period_ms() < 0 {
        return Err(
            ExpireDelegationTokenRequestFailure::InvalidExpiryTimePeriod {
                actual: source.expiry_time_period_ms(),
            },
        );
    }
    Ok(())
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), ExpireDelegationTokenRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(ExpireDelegationTokenRequestFailure::RetainedBytes { required, limit })
}
