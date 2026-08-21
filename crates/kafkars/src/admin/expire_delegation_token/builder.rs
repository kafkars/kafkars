//! Inert secret-bearing delegation-token expiration intent.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, expire_delegation_token::ExpireDelegationTokenAdminRequest,
};

use super::{DelegationTokenHmac, ExpireDelegationToken};

/// Inert request to expire one delegation token.
#[must_use = "call submit to admit the ExpireDelegationToken operation"]
pub struct ExpireDelegationTokenBuilder {
    engine: AdminEngine,
    hmac: DelegationTokenHmac,
    expiry_period: Option<Duration>,
    timeout: Duration,
}

impl ExpireDelegationTokenBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        hmac: DelegationTokenHmac,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            hmac,
            expiry_period: None,
            timeout,
        }
    }

    /// Delays expiration by the supplied period.
    ///
    /// Omitting this method sends Kafka's exact `-1` immediate-expiry sentinel.
    pub const fn expire_after(mut self, period: Duration) -> Self {
        self.expiry_period = Some(period);
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> ExpireDelegationToken {
        let request = ExpireDelegationTokenAdminRequest::new(self.hmac, self.expiry_period);
        ExpireDelegationToken::from_bridge(
            self.engine
                .submit_expire_delegation_token(request, self.timeout),
        )
    }
}

impl std::fmt::Debug for ExpireDelegationTokenBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExpireDelegationTokenBuilder")
            .field("hmac", &self.hmac)
            .field("expiry_period", &self.expiry_period)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
