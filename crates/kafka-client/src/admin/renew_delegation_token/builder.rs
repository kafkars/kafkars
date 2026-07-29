//! Inert secret-bearing delegation-token renewal intent.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, renew_delegation_token::RenewDelegationTokenAdminRequest};

use super::{DelegationTokenHmac, RenewDelegationToken};

/// Inert request to renew one delegation token.
#[must_use = "call submit to admit the RenewDelegationToken operation"]
pub struct RenewDelegationTokenBuilder {
    engine: AdminEngine,
    hmac: DelegationTokenHmac,
    renew_period: Option<Duration>,
    timeout: Duration,
}

impl RenewDelegationTokenBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        hmac: DelegationTokenHmac,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            hmac,
            renew_period: None,
            timeout,
        }
    }

    /// Replaces Kafka's server-default renewal period.
    ///
    /// Omitting this method sends Kafka's exact `-1` default sentinel.
    pub const fn renew_for(mut self, period: Duration) -> Self {
        self.renew_period = Some(period);
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> RenewDelegationToken {
        let request = RenewDelegationTokenAdminRequest::new(self.hmac, self.renew_period);
        RenewDelegationToken::from_bridge(
            self.engine
                .submit_renew_delegation_token(request, self.timeout),
        )
    }
}

impl std::fmt::Debug for RenewDelegationTokenBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RenewDelegationTokenBuilder")
            .field("hmac", &self.hmac)
            .field("renew_period", &self.renew_period)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
