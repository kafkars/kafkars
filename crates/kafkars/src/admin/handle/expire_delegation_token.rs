//! Public entry point for one inert delegation-token expiration.

use super::Admin;
use crate::admin::{DelegationTokenHmac, ExpireDelegationTokenBuilder};

impl Admin {
    /// Builds inert immediate-expiration intent owning one token HMAC.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`ExpireDelegationTokenBuilder::submit`] is called.
    pub fn expire_delegation_token(
        &self,
        hmac: DelegationTokenHmac,
    ) -> ExpireDelegationTokenBuilder {
        ExpireDelegationTokenBuilder::new(self.engine.clone(), hmac, self.engine.default_timeout())
    }
}
