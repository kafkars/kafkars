//! Engine-owned inert owner, renewer, and lifetime intent for API key 38.

use kafka_client_core::{
    CreateDelegationTokenPlan as CorePlan, CreateDelegationTokenPlanError as CorePlanError,
    DelegationTokenPrincipal as CorePrincipal,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateDelegationTokenPlanFailure {
    Invalid,
    RetainedBytes,
}

/// One exact Kafka principal used as token owner or renewer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenPrincipal {
    principal_type: String,
    principal_name: String,
}

impl CreateDelegationTokenPrincipal {
    /// Creates inert principal data. Validation remains deferred until submission.
    pub const fn new(principal_type: String, principal_name: String) -> Self {
        Self {
            principal_type,
            principal_name,
        }
    }

    /// Returns Kafka's exact principal type.
    pub fn principal_type(&self) -> &str {
        &self.principal_type
    }

    /// Returns Kafka's exact principal name.
    pub fn principal_name(&self) -> &str {
        &self.principal_name
    }

    /// Consumes this principal into exact scalar parts.
    pub fn into_parts(self) -> (String, String) {
        (self.principal_type, self.principal_name)
    }

    fn into_core(self) -> Result<CorePrincipal, CorePlanError> {
        let (principal_type, principal_name) = self.into_parts();
        CorePrincipal::new(
            canonical_string(principal_type),
            canonical_string(principal_name),
        )
    }
}

/// One inert delegation-token creation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenRequest {
    owner: Option<CreateDelegationTokenPrincipal>,
    renewers: Vec<CreateDelegationTokenPrincipal>,
    max_lifetime_ms: Option<u64>,
}

impl CreateDelegationTokenRequest {
    /// Creates inert intent. Absence of owner and lifetime uses broker defaults.
    pub const fn new(
        owner: Option<CreateDelegationTokenPrincipal>,
        renewers: Vec<CreateDelegationTokenPrincipal>,
        max_lifetime_ms: Option<u64>,
    ) -> Self {
        Self {
            owner,
            renewers,
            max_lifetime_ms,
        }
    }

    /// Returns the explicit owner, or absence for the authenticated requester.
    pub const fn owner(&self) -> Option<&CreateDelegationTokenPrincipal> {
        self.owner.as_ref()
    }

    /// Returns renewers in exact caller order.
    pub fn renewers(&self) -> &[CreateDelegationTokenPrincipal] {
        &self.renewers
    }

    /// Returns an explicit positive maximum lifetime in milliseconds.
    pub const fn max_lifetime_ms(&self) -> Option<u64> {
        self.max_lifetime_ms
    }

    /// Consumes this request into exact scalar parts.
    pub fn into_parts(
        self,
    ) -> (
        Option<CreateDelegationTokenPrincipal>,
        Vec<CreateDelegationTokenPrincipal>,
        Option<u64>,
    ) {
        (self.owner, self.renewers, self.max_lifetime_ms)
    }

    pub(crate) fn into_plan(self) -> Result<CorePlan, CreateDelegationTokenPlanFailure> {
        let (owner, renewers, max_lifetime_ms) = self.into_parts();
        let owner = owner
            .map(CreateDelegationTokenPrincipal::into_core)
            .transpose()
            .map_err(|_error| CreateDelegationTokenPlanFailure::Invalid)?;
        let mut core_renewers = Vec::new();
        core_renewers
            .try_reserve_exact(renewers.len())
            .map_err(|_| CreateDelegationTokenPlanFailure::RetainedBytes)?;
        for renewer in renewers {
            core_renewers.push(
                renewer
                    .into_core()
                    .map_err(|_error| CreateDelegationTokenPlanFailure::Invalid)?,
            );
        }
        CorePlan::new(owner, core_renewers, max_lifetime_ms)
            .map_err(|_error| CreateDelegationTokenPlanFailure::Invalid)
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}
