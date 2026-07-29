//! Test-only generated-free constructors for host response evidence.

use super::{
    DelegationTokenHmac, NormalizedCreateDelegationTokenResponse, NormalizedDelegationToken,
    NormalizedDelegationTokenPrincipal,
};

impl NormalizedDelegationTokenPrincipal {
    pub(crate) const fn fixture(principal_type: String, principal_name: String) -> Self {
        Self::new(principal_type, principal_name)
    }
}

impl NormalizedDelegationToken {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn fixture(
        owner: NormalizedDelegationTokenPrincipal,
        requester: Option<NormalizedDelegationTokenPrincipal>,
        issue_timestamp_ms: i64,
        expiry_timestamp_ms: i64,
        max_timestamp_ms: i64,
        token_id: String,
        hmac: Vec<u8>,
    ) -> Self {
        Self::new(
            owner,
            requester,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            DelegationTokenHmac::new(hmac),
        )
    }
}

impl NormalizedCreateDelegationTokenResponse {
    pub(crate) const fn fixture(
        throttle_time_ms: u32,
        broker_error_code: i16,
        token: Option<NormalizedDelegationToken>,
        retained_bytes: usize,
    ) -> Self {
        Self::new(throttle_time_ms, broker_error_code, token, retained_bytes)
    }
}
