//! Validate-first API-key 39 secret ownership and period adaptation.

use super::{
    PreparedRenewDelegationTokenRequest, RenewDelegationTokenRequestRef,
    retention::{MAX_HMAC_BYTES, RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES, request_peak_charge},
    secret::RenewDelegationTokenHmac,
};

/// Invalid intent, allocation failure, or insufficient retained capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RenewDelegationTokenRequestFailure {
    EmptyHmac,
    HmacTooLong {
        actual: usize,
        max: usize,
    },
    InvalidRenewPeriod {
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
pub(crate) fn renew_delegation_token_request(
    source: RenewDelegationTokenRequestRef<'_>,
    retained_limit: usize,
) -> Result<PreparedRenewDelegationTokenRequest, RenewDelegationTokenRequestFailure> {
    validate(source)?;
    let effective_limit = retained_limit.min(RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES);
    let required = request_peak_charge(source).unwrap_or(usize::MAX);
    ensure_limit(required, effective_limit)?;
    let hmac = RenewDelegationTokenHmac::copy_from(source.hmac()).map_err(|()| {
        RenewDelegationTokenRequestFailure::Allocation {
            field: "hmac",
            requested: source.hmac().len(),
        }
    })?;
    let prepared = PreparedRenewDelegationTokenRequest::new(hmac, source.renew_period_ms());
    ensure_limit(prepared.retained_heap_bytes(), effective_limit)?;
    Ok(prepared)
}

fn validate(
    source: RenewDelegationTokenRequestRef<'_>,
) -> Result<(), RenewDelegationTokenRequestFailure> {
    if source.hmac().is_empty() {
        return Err(RenewDelegationTokenRequestFailure::EmptyHmac);
    }
    if source.hmac().len() > MAX_HMAC_BYTES {
        return Err(RenewDelegationTokenRequestFailure::HmacTooLong {
            actual: source.hmac().len(),
            max: MAX_HMAC_BYTES,
        });
    }
    if source.renew_period_ms() != -1 && source.renew_period_ms() <= 0 {
        return Err(RenewDelegationTokenRequestFailure::InvalidRenewPeriod {
            actual: source.renew_period_ms(),
        });
    }
    Ok(())
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), RenewDelegationTokenRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(RenewDelegationTokenRequestFailure::RetainedBytes { required, limit })
}
