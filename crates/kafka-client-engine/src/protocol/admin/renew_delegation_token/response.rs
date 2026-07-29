//! Allocation-free normalization of one API-key 39 scalar terminal.

use kafka_wire::RenewDelegationTokenResponse;

use super::{
    NormalizedRenewDelegationTokenResponse,
    retention::{RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES, response_charge},
};

const MIN_VERSION: i16 = 1;
const MAX_VERSION: i16 = 2;

/// Invalid selected version, scalar shape, or terminal capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RenewDelegationTokenResponseFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    InvalidExpiryTimestamp { actual: i64 },
    RetainedBytes { required: usize, limit: usize },
}

/// Preserves exact signed broker status and validates success expiry.
pub(crate) fn normalize_renew_delegation_token_response(
    selected_version: Option<i16>,
    response: &RenewDelegationTokenResponse,
    retained_limit: usize,
) -> Result<NormalizedRenewDelegationTokenResponse, RenewDelegationTokenResponseFailure> {
    let selected_version =
        selected_version.ok_or(RenewDelegationTokenResponseFailure::MissingSelectedVersion)?;
    if !(MIN_VERSION..=MAX_VERSION).contains(&selected_version) {
        return Err(RenewDelegationTokenResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        RenewDelegationTokenResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let expiry_timestamp_ms = if response.error_code == 0 {
        if response.expiry_timestamp_ms < 0 {
            return Err(
                RenewDelegationTokenResponseFailure::InvalidExpiryTimestamp {
                    actual: response.expiry_timestamp_ms,
                },
            );
        }
        Some(response.expiry_timestamp_ms)
    } else {
        None
    };
    let required = response_charge();
    let effective_limit = retained_limit.min(RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES);
    if required > effective_limit {
        return Err(RenewDelegationTokenResponseFailure::RetainedBytes {
            required,
            limit: effective_limit,
        });
    }
    Ok(NormalizedRenewDelegationTokenResponse::new(
        throttle_time_ms,
        response.error_code,
        expiry_timestamp_ms,
        required,
    ))
}
