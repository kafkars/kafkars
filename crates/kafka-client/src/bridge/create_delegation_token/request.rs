//! Capture-after conversion of inert public delegation-token intent.

use std::time::Duration;

use crate::admin::DelegationTokenPrincipal;

use super::engine::{Principal as EnginePrincipal, Request as EngineRequest};

/// Facade values retained without validation before public submission.
#[derive(Debug)]
pub(crate) struct CreateDelegationTokenAdminRequest {
    owner: Option<DelegationTokenPrincipal>,
    renewers: Vec<DelegationTokenPrincipal>,
    max_lifetime: Option<Duration>,
}

impl CreateDelegationTokenAdminRequest {
    pub(crate) const fn new(
        owner: Option<DelegationTokenPrincipal>,
        renewers: Vec<DelegationTokenPrincipal>,
        max_lifetime: Option<Duration>,
    ) -> Self {
        Self {
            owner,
            renewers,
            max_lifetime,
        }
    }

    /// Converts only after the engine has captured the public deadline.
    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.owner.map(translate_principal),
            self.renewers.into_iter().map(translate_principal).collect(),
            self.max_lifetime.map(duration_millis),
        )
    }
}

fn translate_principal(principal: DelegationTokenPrincipal) -> EnginePrincipal {
    let (principal_type, principal_name) = principal.into_parts();
    EnginePrincipal::new(principal_type, principal_name)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
