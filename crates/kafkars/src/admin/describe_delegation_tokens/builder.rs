//! Inert delegation-token description intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, describe_delegation_tokens::DescribeDelegationTokensAdminRequest,
};

use super::{DelegationTokenPrincipal, DescribeDelegationTokens};

/// Inert query for delegation tokens visible to the authenticated principal.
///
/// With no explicit owner filter, the query selects all visible tokens.
#[must_use = "call submit to admit the DescribeDelegationTokens operation"]
pub struct DescribeDelegationTokensBuilder {
    engine: AdminEngine,
    request: DescribeDelegationTokensAdminRequest,
    timeout: Duration,
}

impl DescribeDelegationTokensBuilder {
    pub(crate) const fn new(engine: AdminEngine, timeout: Duration) -> Self {
        Self {
            engine,
            request: DescribeDelegationTokensAdminRequest::all(),
            timeout,
        }
    }

    /// Replaces the selection with caller-ordered token owners.
    ///
    /// Construction remains inert. Empty and duplicate filters are rejected
    /// only when [`Self::submit`] reaches bounded engine admission; they never
    /// silently become the distinct all-visible-tokens selection.
    pub fn owners<I>(mut self, owners: I) -> Self
    where
        I: IntoIterator<Item = DelegationTokenPrincipal>,
    {
        self.request = DescribeDelegationTokensAdminRequest::owners(owners.into_iter().collect());
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> DescribeDelegationTokens {
        DescribeDelegationTokens::from_bridge(
            self.engine
                .submit_describe_delegation_tokens(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeDelegationTokensBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeDelegationTokensBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
