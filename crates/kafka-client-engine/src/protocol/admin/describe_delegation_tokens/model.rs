//! Borrowed owner selection and generated-free secret-bearing token facts.

use core::fmt;

use super::DescribeDelegationTokenHmac;

/// One borrowed Kafka principal used to select described tokens.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DescribeDelegationTokenPrincipalRef<'a> {
    principal_type: &'a str,
    principal_name: &'a str,
}

impl<'a> DescribeDelegationTokenPrincipalRef<'a> {
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

/// Explicit API-key 41 selection; absence means all visible tokens.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DescribeDelegationTokensRequestRef<'a> {
    owners: Option<&'a [DescribeDelegationTokenPrincipalRef<'a>]>,
}

impl<'a> DescribeDelegationTokensRequestRef<'a> {
    pub(crate) const fn all() -> Self {
        Self { owners: None }
    }

    pub(crate) const fn selected(owners: &'a [DescribeDelegationTokenPrincipalRef<'a>]) -> Self {
        Self {
            owners: Some(owners),
        }
    }

    pub(crate) const fn owners(self) -> Option<&'a [DescribeDelegationTokenPrincipalRef<'a>]> {
        self.owners
    }
}

/// One owned principal from a normalized API-key 41 response.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct NormalizedDescribeDelegationTokenPrincipal {
    principal_type: String,
    principal_name: String,
}

impl NormalizedDescribeDelegationTokenPrincipal {
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

/// One complete described token with a zeroizing HMAC owner.
pub(crate) struct NormalizedDescribedDelegationToken {
    owner: NormalizedDescribeDelegationTokenPrincipal,
    requester: Option<NormalizedDescribeDelegationTokenPrincipal>,
    issue_timestamp_ms: i64,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
    token_id: String,
    hmac: DescribeDelegationTokenHmac,
    renewers: Vec<NormalizedDescribeDelegationTokenPrincipal>,
}

impl NormalizedDescribedDelegationToken {
    #[allow(clippy::too_many_arguments)]
    pub(super) const fn new(
        owner: NormalizedDescribeDelegationTokenPrincipal,
        requester: Option<NormalizedDescribeDelegationTokenPrincipal>,
        issue_timestamp_ms: i64,
        expiry_timestamp_ms: i64,
        max_timestamp_ms: i64,
        token_id: String,
        hmac: DescribeDelegationTokenHmac,
        renewers: Vec<NormalizedDescribeDelegationTokenPrincipal>,
    ) -> Self {
        Self {
            owner,
            requester,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
            renewers,
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn into_parts(
        self,
    ) -> (
        NormalizedDescribeDelegationTokenPrincipal,
        Option<NormalizedDescribeDelegationTokenPrincipal>,
        i64,
        i64,
        i64,
        String,
        DescribeDelegationTokenHmac,
        Vec<NormalizedDescribeDelegationTokenPrincipal>,
    ) {
        (
            self.owner,
            self.requester,
            self.issue_timestamp_ms,
            self.expiry_timestamp_ms,
            self.max_timestamp_ms,
            self.token_id,
            self.hmac,
            self.renewers,
        )
    }

    pub(super) fn retained_capacity(&self) -> Option<usize> {
        self.owner
            .retained_capacity()?
            .checked_add(self.requester.as_ref().map_or(
                Some(0),
                NormalizedDescribeDelegationTokenPrincipal::retained_capacity,
            )?)?
            .checked_add(self.token_id.capacity())?
            .checked_add(self.hmac.retained_capacity())?
            .checked_add(self.renewers.iter().try_fold(0usize, |bytes, principal| {
                bytes.checked_add(principal.retained_capacity()?)
            })?)?
            .checked_add(self.renewers.capacity().checked_mul(core::mem::size_of::<
                NormalizedDescribeDelegationTokenPrincipal,
            >())?)
    }
}

impl fmt::Debug for NormalizedDescribedDelegationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NormalizedDescribedDelegationToken")
            .field("owner", &self.owner)
            .field("requester", &self.requester)
            .field("issue_timestamp_ms", &self.issue_timestamp_ms)
            .field("expiry_timestamp_ms", &self.expiry_timestamp_ms)
            .field("max_timestamp_ms", &self.max_timestamp_ms)
            .field("token_id", &self.token_id)
            .field("hmac", &self.hmac)
            .field("renewers", &self.renewers)
            .finish()
    }
}

/// One complete bounded token list with exact signed broker status.
pub(crate) struct NormalizedDescribeDelegationTokensResponse {
    throttle_time_ms: u32,
    broker_error_code: i16,
    tokens: Vec<NormalizedDescribedDelegationToken>,
    retained_bytes: usize,
}

impl NormalizedDescribeDelegationTokensResponse {
    pub(super) const fn new(
        throttle_time_ms: u32,
        broker_error_code: i16,
        tokens: Vec<NormalizedDescribedDelegationToken>,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            broker_error_code,
            tokens,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (u32, i16, Vec<NormalizedDescribedDelegationToken>, usize) {
        (
            self.throttle_time_ms,
            self.broker_error_code,
            self.tokens,
            self.retained_bytes,
        )
    }

    pub(super) fn tokens(&self) -> &[NormalizedDescribedDelegationToken] {
        &self.tokens
    }

    pub(super) fn token_capacity(&self) -> usize {
        self.tokens.capacity()
    }
}

impl fmt::Debug for NormalizedDescribeDelegationTokensResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NormalizedDescribeDelegationTokensResponse")
            .field("throttle_time_ms", &self.throttle_time_ms)
            .field("broker_error_code", &self.broker_error_code)
            .field("tokens", &self.tokens)
            .field("retained_bytes", &self.retained_bytes)
            .finish()
    }
}
