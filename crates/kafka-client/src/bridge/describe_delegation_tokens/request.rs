//! Explicit all-or-owner-filter token selection converted after deadline capture.

use crate::admin::DelegationTokenPrincipal;

use super::engine::{Principal as EnginePrincipal, Request as EngineRequest};

enum Selection {
    All,
    Owners(Vec<DelegationTokenPrincipal>),
}

/// Facade values retained without validation before public submission.
pub(crate) struct DescribeDelegationTokensAdminRequest {
    selection: Selection,
}

impl DescribeDelegationTokensAdminRequest {
    pub(crate) const fn all() -> Self {
        Self {
            selection: Selection::All,
        }
    }

    pub(crate) const fn owners(owners: Vec<DelegationTokenPrincipal>) -> Self {
        Self {
            selection: Selection::Owners(owners),
        }
    }

    /// Converts only after the engine has captured the public deadline.
    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        match self.selection {
            Selection::All => EngineRequest::all(),
            Selection::Owners(owners) => {
                EngineRequest::for_owners(owners.into_iter().map(translate_principal).collect())
            }
        }
    }
}

impl std::fmt::Debug for DescribeDelegationTokensAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.selection {
            Selection::All => formatter
                .debug_struct("DescribeDelegationTokensAdminRequest")
                .field("selection", &"all")
                .finish(),
            Selection::Owners(owners) => formatter
                .debug_struct("DescribeDelegationTokensAdminRequest")
                .field("owners", owners)
                .finish(),
        }
    }
}

fn translate_principal(principal: DelegationTokenPrincipal) -> EnginePrincipal {
    let (principal_type, principal_name) = principal.into_parts();
    EnginePrincipal::new(principal_type, principal_name)
}
