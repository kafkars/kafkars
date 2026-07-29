//! Engine-owned inert owner selection for API key 41.

use kafka_client_core::{
    DelegationTokenPrincipal as CorePrincipal, DescribeDelegationTokensPlan as CorePlan,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeDelegationTokensPlanFailure {
    Invalid,
    RetainedBytes,
}

/// One exact Kafka principal used as a token-owner filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokenPrincipal {
    principal_type: String,
    principal_name: String,
}

impl DescribeDelegationTokenPrincipal {
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

    fn to_core(&self) -> Result<CorePrincipal, DescribeDelegationTokensPlanFailure> {
        CorePrincipal::new(
            canonical_string(&self.principal_type),
            canonical_string(&self.principal_name),
        )
        .map_err(|_error| DescribeDelegationTokensPlanFailure::Invalid)
    }
}

/// Inert all-visible or explicit caller-ordered owner selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokensRequest {
    owners: Option<Vec<DescribeDelegationTokenPrincipal>>,
}

impl DescribeDelegationTokensRequest {
    /// Selects every delegation token visible to the authenticated principal.
    pub const fn all() -> Self {
        Self { owners: None }
    }

    /// Selects tokens for explicit owners in caller order.
    pub const fn for_owners(owners: Vec<DescribeDelegationTokenPrincipal>) -> Self {
        Self {
            owners: Some(owners),
        }
    }

    /// Returns explicit owners, or `None` for all visible tokens.
    pub fn owners(&self) -> Option<&[DescribeDelegationTokenPrincipal]> {
        self.owners.as_deref()
    }

    /// Consumes this request into its exact optional owner selection.
    pub fn into_owners(self) -> Option<Vec<DescribeDelegationTokenPrincipal>> {
        self.owners
    }

    pub(crate) fn plan(&self) -> Result<CorePlan, DescribeDelegationTokensPlanFailure> {
        let Some(owners) = &self.owners else {
            return Ok(CorePlan::all());
        };
        let mut core_owners = Vec::new();
        core_owners
            .try_reserve_exact(owners.len())
            .map_err(|_| DescribeDelegationTokensPlanFailure::RetainedBytes)?;
        for owner in owners {
            core_owners.push(owner.to_core()?);
        }
        CorePlan::for_owners(core_owners)
            .map_err(|_error| DescribeDelegationTokensPlanFailure::Invalid)
    }
}

fn canonical_string(value: &str) -> String {
    value.to_owned().into_boxed_str().into_string()
}
