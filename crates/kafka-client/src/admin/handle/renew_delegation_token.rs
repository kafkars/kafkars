//! Public entry point for one inert delegation-token renewal.

use super::Admin;
use crate::admin::{DelegationTokenHmac, RenewDelegationTokenBuilder};

impl Admin {
    /// Builds inert renewal intent that uniquely owns one token HMAC.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`RenewDelegationTokenBuilder::submit`] is called.
    pub fn renew_delegation_token(&self, hmac: DelegationTokenHmac) -> RenewDelegationTokenBuilder {
        RenewDelegationTokenBuilder::new(self.engine.clone(), hmac, self.engine.default_timeout())
    }
}
