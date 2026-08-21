//! Secret-bearing expiration intent converted only after deadline capture.

use std::time::Duration;

use crate::admin::DelegationTokenHmac;

use super::engine::{Hmac as EngineHmac, Request as EngineRequest};

/// Facade values retained without validation before public submission.
pub(crate) struct ExpireDelegationTokenAdminRequest {
    hmac: DelegationTokenHmac,
    expiry_period: Option<Duration>,
}

impl ExpireDelegationTokenAdminRequest {
    pub(crate) const fn new(hmac: DelegationTokenHmac, expiry_period: Option<Duration>) -> Self {
        Self {
            hmac,
            expiry_period,
        }
    }

    /// Converts only after the engine has captured the public deadline.
    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            EngineHmac::new(self.hmac.into_bytes()),
            self.expiry_period.map(duration_millis),
        )
    }
}

impl std::fmt::Debug for ExpireDelegationTokenAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExpireDelegationTokenAdminRequest")
            .field("hmac", &self.hmac)
            .field("expiry_period", &self.expiry_period)
            .finish()
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
