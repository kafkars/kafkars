//! Inert delegation-token creation intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, create_delegation_token::CreateDelegationTokenAdminRequest,
};

use super::{CreateDelegationToken, DelegationTokenPrincipal};

/// Inert request for one broker-created delegation token.
#[must_use = "call submit to admit the CreateDelegationToken operation"]
pub struct CreateDelegationTokenBuilder {
    engine: AdminEngine,
    owner: Option<DelegationTokenPrincipal>,
    renewers: Vec<DelegationTokenPrincipal>,
    max_lifetime: Option<Duration>,
    timeout: Duration,
}

impl CreateDelegationTokenBuilder {
    pub(crate) const fn new(engine: AdminEngine, timeout: Duration) -> Self {
        Self {
            engine,
            owner: None,
            renewers: Vec::new(),
            max_lifetime: None,
            timeout,
        }
    }

    /// Replaces the explicit token owner.
    ///
    /// Absence leaves owner selection to the authenticated requester.
    pub fn owner(mut self, owner: DelegationTokenPrincipal) -> Self {
        self.owner = Some(owner);
        self
    }

    /// Replaces the caller-ordered token renewers.
    pub fn renewers<I>(mut self, renewers: I) -> Self
    where
        I: IntoIterator<Item = DelegationTokenPrincipal>,
    {
        self.renewers = renewers.into_iter().collect();
        self
    }

    /// Replaces the explicit maximum lifetime.
    ///
    /// Omitting this method preserves Kafka's server-default lifetime.
    pub const fn max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = Some(lifetime);
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    ///
    /// Principal, renewer, lifetime, and version validation remains deferred
    /// until this sole public operation boundary.
    pub fn submit(self) -> CreateDelegationToken {
        let request =
            CreateDelegationTokenAdminRequest::new(self.owner, self.renewers, self.max_lifetime);
        CreateDelegationToken::from_bridge(
            self.engine
                .submit_create_delegation_token(request, self.timeout),
        )
    }
}

impl std::fmt::Debug for CreateDelegationTokenBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CreateDelegationTokenBuilder")
            .field("owner", &self.owner)
            .field("renewers", &self.renewers)
            .field("max_lifetime", &self.max_lifetime)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
