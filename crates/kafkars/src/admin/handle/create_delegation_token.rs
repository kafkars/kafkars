//! Public entry point for one inert delegation-token creation.

use super::Admin;
use crate::admin::CreateDelegationTokenBuilder;

impl Admin {
    /// Builds inert delegation-token creation intent.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`CreateDelegationTokenBuilder::submit`] is called.
    pub fn create_delegation_token(&self) -> CreateDelegationTokenBuilder {
        CreateDelegationTokenBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }
}
