//! Secret-bearing renewal intent converted only after deadline capture.

use std::time::Duration;

use crate::admin::DelegationTokenHmac;

use super::engine::{Hmac as EngineHmac, Request as EngineRequest};

/// Facade values retained without validation before public submission.
pub(crate) struct RenewDelegationTokenAdminRequest {
    hmac: DelegationTokenHmac,
    renew_period: Option<Duration>,
}

impl RenewDelegationTokenAdminRequest {
    pub(crate) const fn new(hmac: DelegationTokenHmac, renew_period: Option<Duration>) -> Self {
        Self { hmac, renew_period }
    }

    /// Converts only after the engine has captured the public deadline.
    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            EngineHmac::new(self.hmac.into_bytes()),
            self.renew_period.map(duration_millis),
        )
    }
}

impl std::fmt::Debug for RenewDelegationTokenAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RenewDelegationTokenAdminRequest")
            .field("hmac", &self.hmac)
            .field("renew_period", &self.renew_period)
            .finish()
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
