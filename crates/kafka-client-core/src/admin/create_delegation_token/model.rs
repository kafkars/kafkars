//! Bounded principal, renewer, and lifetime intent for API 38.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum UTF-8 bytes accepted in one principal type or name.
pub const CREATE_DELEGATION_TOKEN_MAX_PRINCIPAL_BYTES: usize = i16::MAX as usize;
/// Maximum caller-ordered renewers retained by one token request.
pub const CREATE_DELEGATION_TOKEN_MAX_RENEWERS: usize = 4 * 1024;
/// Maximum aggregate principal text retained by one token request.
pub const CREATE_DELEGATION_TOKEN_MAX_REQUEST_TEXT_BYTES: usize = 256 * 1024;

/// One exact Kafka principal represented without generated protocol values.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DelegationTokenPrincipal {
    principal_type: String,
    principal_name: String,
}

impl DelegationTokenPrincipal {
    /// Validates one nonempty bounded principal type and name.
    pub fn new(
        principal_type: String,
        principal_name: String,
    ) -> Result<Self, CreateDelegationTokenPlanError> {
        if principal_type.is_empty() {
            return Err(CreateDelegationTokenPlanError::EmptyPrincipalType);
        }
        if principal_type.len() > CREATE_DELEGATION_TOKEN_MAX_PRINCIPAL_BYTES {
            return Err(CreateDelegationTokenPlanError::PrincipalTypeTooLong);
        }
        if principal_name.is_empty() {
            return Err(CreateDelegationTokenPlanError::EmptyPrincipalName);
        }
        if principal_name.len() > CREATE_DELEGATION_TOKEN_MAX_PRINCIPAL_BYTES {
            return Err(CreateDelegationTokenPlanError::PrincipalNameTooLong);
        }
        Ok(Self {
            principal_type,
            principal_name,
        })
    }

    /// Returns Kafka's exact principal type.
    pub fn principal_type(&self) -> &str {
        &self.principal_type
    }

    /// Returns Kafka's exact principal name.
    pub fn principal_name(&self) -> &str {
        &self.principal_name
    }

    /// Consumes the principal into adapter-owned scalar parts.
    pub fn into_parts(self) -> (String, String) {
        (self.principal_type, self.principal_name)
    }
}

/// Validated intent for one single-attempt token creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenPlan {
    owner: Option<DelegationTokenPrincipal>,
    renewers: Vec<DelegationTokenPrincipal>,
    max_lifetime_ms: Option<u64>,
}

impl CreateDelegationTokenPlan {
    /// Validates optional v3 owner, ordered unique renewers, and lifetime.
    pub fn new(
        owner: Option<DelegationTokenPrincipal>,
        renewers: Vec<DelegationTokenPrincipal>,
        max_lifetime_ms: Option<u64>,
    ) -> Result<Self, CreateDelegationTokenPlanError> {
        if renewers.len() > CREATE_DELEGATION_TOKEN_MAX_RENEWERS {
            return Err(CreateDelegationTokenPlanError::TooManyRenewers);
        }
        if max_lifetime_ms == Some(0) {
            return Err(CreateDelegationTokenPlanError::ZeroMaxLifetime);
        }
        if max_lifetime_ms.is_some_and(|value| value > i64::MAX as u64) {
            return Err(CreateDelegationTokenPlanError::MaxLifetimeTooLarge);
        }

        let mut retained_text = owner.as_ref().map_or(0, principal_text_bytes);
        let mut identities = BTreeSet::new();
        for renewer in &renewers {
            if !identities.insert(renewer) {
                return Err(CreateDelegationTokenPlanError::DuplicateRenewer);
            }
            retained_text = retained_text
                .checked_add(principal_text_bytes(renewer))
                .ok_or(CreateDelegationTokenPlanError::RequestTextBytesExceeded)?;
            if retained_text > CREATE_DELEGATION_TOKEN_MAX_REQUEST_TEXT_BYTES {
                return Err(CreateDelegationTokenPlanError::RequestTextBytesExceeded);
            }
        }

        Ok(Self {
            owner,
            renewers,
            max_lifetime_ms,
        })
    }

    /// Returns the explicit token owner, or absence for the requester.
    pub const fn owner(&self) -> Option<&DelegationTokenPrincipal> {
        self.owner.as_ref()
    }

    /// Returns renewers in exact caller order.
    pub fn renewers(&self) -> &[DelegationTokenPrincipal] {
        &self.renewers
    }

    /// Returns an explicit positive lifetime, or absence for broker default.
    pub const fn max_lifetime_ms(&self) -> Option<u64> {
        self.max_lifetime_ms
    }

    /// Returns the oldest API version that represents the exact owner intent.
    pub const fn minimum_version(&self) -> i16 {
        if self.owner.is_some() { 3 } else { 1 }
    }

    /// Consumes this plan into adapter-owned request parts.
    pub fn into_parts(
        self,
    ) -> (
        Option<DelegationTokenPrincipal>,
        Vec<DelegationTokenPrincipal>,
        Option<u64>,
    ) {
        (self.owner, self.renewers, self.max_lifetime_ms)
    }
}

fn principal_text_bytes(principal: &DelegationTokenPrincipal) -> usize {
    principal.principal_type.len() + principal.principal_name.len()
}

/// Invalid deterministic token-creation intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenPlanError {
    /// Principal types cannot be empty.
    EmptyPrincipalType,
    /// Principal types must fit Kafka's string domain.
    PrincipalTypeTooLong,
    /// Principal names cannot be empty.
    EmptyPrincipalName,
    /// Principal names must fit Kafka's string domain.
    PrincipalNameTooLong,
    /// One request retained more renewers than its deterministic bound.
    TooManyRenewers,
    /// One request cannot repeat an exact renewer principal.
    DuplicateRenewer,
    /// An explicit lifetime must be positive.
    ZeroMaxLifetime,
    /// An explicit lifetime must fit Kafka's signed millisecond field.
    MaxLifetimeTooLarge,
    /// Aggregate principal text exceeded the deterministic request bound.
    RequestTextBytesExceeded,
}

impl fmt::Display for CreateDelegationTokenPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid CreateDelegationToken plan: {self:?}")
    }
}

impl std::error::Error for CreateDelegationTokenPlanError {}
