//! Generated-free request views and secret-safe normalized token facts.

use core::fmt;

use super::secret::DelegationTokenHmac;

/// One borrowed Kafka principal supplied as owner or renewer intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DelegationTokenPrincipalRef<'a> {
    principal_type: &'a str,
    principal_name: &'a str,
}

impl<'a> DelegationTokenPrincipalRef<'a> {
    pub(crate) const fn new(principal_type: &'a str, principal_name: &'a str) -> Self {
        Self {
            principal_type,
            principal_name,
        }
    }

    pub(crate) const fn principal_type(self) -> &'a str {
        self.principal_type
    }

    pub(crate) const fn principal_name(self) -> &'a str {
        self.principal_name
    }
}

/// Borrowed API-key 38 intent captured before generated request ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CreateDelegationTokenRequestRef<'a> {
    owner: Option<DelegationTokenPrincipalRef<'a>>,
    renewers: &'a [DelegationTokenPrincipalRef<'a>],
    max_lifetime_ms: i64,
}

impl<'a> CreateDelegationTokenRequestRef<'a> {
    pub(crate) const fn new(
        owner: Option<DelegationTokenPrincipalRef<'a>>,
        renewers: &'a [DelegationTokenPrincipalRef<'a>],
        max_lifetime_ms: i64,
    ) -> Self {
        Self {
            owner,
            renewers,
            max_lifetime_ms,
        }
    }

    pub(crate) const fn owner(self) -> Option<DelegationTokenPrincipalRef<'a>> {
        self.owner
    }

    pub(crate) const fn renewers(self) -> &'a [DelegationTokenPrincipalRef<'a>] {
        self.renewers
    }

    pub(crate) const fn max_lifetime_ms(self) -> i64 {
        self.max_lifetime_ms
    }
}

/// One owned, validated principal from a successful token response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDelegationTokenPrincipal {
    principal_type: String,
    principal_name: String,
}

impl NormalizedDelegationTokenPrincipal {
    pub(super) const fn new(principal_type: String, principal_name: String) -> Self {
        Self {
            principal_type,
            principal_name,
        }
    }

    pub(crate) fn into_parts(self) -> (String, String) {
        (self.principal_type, self.principal_name)
    }

    pub(super) fn retained_capacity(&self) -> Option<usize> {
        self.principal_type
            .capacity()
            .checked_add(self.principal_name.capacity())
    }
}

/// Successful token material separated from the exact top-level broker code.
pub(crate) struct NormalizedDelegationToken {
    owner: NormalizedDelegationTokenPrincipal,
    requester: Option<NormalizedDelegationTokenPrincipal>,
    issue_timestamp_ms: i64,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
    token_id: String,
    hmac: DelegationTokenHmac,
}

impl NormalizedDelegationToken {
    #[allow(clippy::too_many_arguments)]
    pub(super) const fn new(
        owner: NormalizedDelegationTokenPrincipal,
        requester: Option<NormalizedDelegationTokenPrincipal>,
        issue_timestamp_ms: i64,
        expiry_timestamp_ms: i64,
        max_timestamp_ms: i64,
        token_id: String,
        hmac: DelegationTokenHmac,
    ) -> Self {
        Self {
            owner,
            requester,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn into_parts(
        self,
    ) -> (
        NormalizedDelegationTokenPrincipal,
        Option<NormalizedDelegationTokenPrincipal>,
        i64,
        i64,
        i64,
        String,
        DelegationTokenHmac,
    ) {
        (
            self.owner,
            self.requester,
            self.issue_timestamp_ms,
            self.expiry_timestamp_ms,
            self.max_timestamp_ms,
            self.token_id,
            self.hmac,
        )
    }

    pub(super) fn retained_capacity(&self) -> Option<usize> {
        self.owner
            .retained_capacity()?
            .checked_add(self.requester.as_ref().map_or(
                Some(0),
                NormalizedDelegationTokenPrincipal::retained_capacity,
            )?)?
            .checked_add(self.token_id.capacity())?
            .checked_add(self.hmac.retained_capacity())
    }
}

impl fmt::Debug for NormalizedDelegationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NormalizedDelegationToken")
            .field("owner", &self.owner)
            .field("requester", &self.requester)
            .field("issue_timestamp_ms", &self.issue_timestamp_ms)
            .field("expiry_timestamp_ms", &self.expiry_timestamp_ms)
            .field("max_timestamp_ms", &self.max_timestamp_ms)
            .field("token_id", &self.token_id)
            .field("hmac", &self.hmac)
            .finish()
    }
}

/// One bounded API-key 38 terminal preserving the signed broker status.
pub(crate) struct NormalizedCreateDelegationTokenResponse {
    throttle_time_ms: u32,
    broker_error_code: i16,
    token: Option<NormalizedDelegationToken>,
    retained_bytes: usize,
}

impl NormalizedCreateDelegationTokenResponse {
    pub(super) const fn new(
        throttle_time_ms: u32,
        broker_error_code: i16,
        token: Option<NormalizedDelegationToken>,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            broker_error_code,
            token,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (u32, i16, Option<NormalizedDelegationToken>, usize) {
        (
            self.throttle_time_ms,
            self.broker_error_code,
            self.token,
            self.retained_bytes,
        )
    }

    pub(super) fn token(&self) -> Option<&NormalizedDelegationToken> {
        self.token.as_ref()
    }
}

impl fmt::Debug for NormalizedCreateDelegationTokenResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NormalizedCreateDelegationTokenResponse")
            .field("throttle_time_ms", &self.throttle_time_ms)
            .field("broker_error_code", &self.broker_error_code)
            .field("token", &self.token)
            .field("retained_bytes", &self.retained_bytes)
            .finish()
    }
}
