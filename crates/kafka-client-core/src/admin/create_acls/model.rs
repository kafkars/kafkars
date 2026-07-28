//! Validated concrete ACL bindings for one bounded creation batch.

use core::fmt;
use std::collections::BTreeSet;

const MAX_ACL_STRING_BYTES: usize = i16::MAX as usize;

/// Maximum number of bindings accepted by one deterministic creation plan.
pub const MAX_CREATE_ACLS_BINDINGS: usize = 16 * 1024;

/// One concrete wire-free ACL binding using exact protocol-domain scalars.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclBinding {
    resource_type: i8,
    resource_name: String,
    pattern_type: i8,
    principal: String,
    host: String,
    operation: i8,
    permission_type: i8,
}

impl CreateAclBinding {
    /// Creates inert binding intent for validation by the enclosing plan.
    pub const fn new(
        resource_type: i8,
        resource_name: String,
        pattern_type: i8,
        principal: String,
        host: String,
        operation: i8,
        permission_type: i8,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        }
    }

    /// Returns Kafka's exact concrete resource type.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the nonempty concrete resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns Kafka's exact concrete resource-pattern type.
    pub const fn pattern_type(&self) -> i8 {
        self.pattern_type
    }

    /// Returns the nonempty principal identity.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the nonempty host identity or explicit wildcard.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns Kafka's exact concrete operation.
    pub const fn operation(&self) -> i8 {
        self.operation
    }

    /// Returns Kafka's exact concrete permission type.
    pub const fn permission_type(&self) -> i8 {
        self.permission_type
    }

    /// Consumes this binding into adapter-owned scalar parts.
    pub fn into_parts(self) -> (i8, String, i8, String, String, i8, i8) {
        (
            self.resource_type,
            self.resource_name,
            self.pattern_type,
            self.principal,
            self.host,
            self.operation,
            self.permission_type,
        )
    }
}

/// Validated caller-ordered intent for one bounded `CreateAcls` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclsPlan {
    bindings: Vec<CreateAclBinding>,
}

impl CreateAclsPlan {
    /// Validates a nonempty bounded batch of unique concrete bindings.
    pub fn new(bindings: Vec<CreateAclBinding>) -> Result<Self, CreateAclsPlanError> {
        if bindings.is_empty() {
            return Err(CreateAclsPlanError::EmptyBatch);
        }
        if bindings.len() > MAX_CREATE_ACLS_BINDINGS {
            return Err(CreateAclsPlanError::BatchTooLarge);
        }
        let mut identities = BTreeSet::new();
        for binding in &bindings {
            validate_binding(binding)?;
            if !identities.insert((
                binding.resource_type,
                binding.resource_name.as_str(),
                binding.pattern_type,
                binding.principal.as_str(),
                binding.host.as_str(),
                binding.operation,
                binding.permission_type,
            )) {
                return Err(CreateAclsPlanError::DuplicateBinding);
            }
        }
        drop(identities);
        Ok(Self { bindings })
    }

    /// Returns concrete bindings in exact caller order.
    pub fn bindings(&self) -> &[CreateAclBinding] {
        &self.bindings
    }

    /// Returns the exact result slots an adapter must reserve before admission.
    pub const fn required_result_capacity(&self) -> usize {
        self.bindings.len()
    }

    pub(crate) fn into_bindings(self) -> Vec<CreateAclBinding> {
        self.bindings
    }
}

fn validate_binding(binding: &CreateAclBinding) -> Result<(), CreateAclsPlanError> {
    if binding.resource_type < 2 {
        return Err(CreateAclsPlanError::InvalidResourceType);
    }
    validate_string(
        &binding.resource_name,
        CreateAclsPlanError::EmptyResourceName,
        CreateAclsPlanError::ResourceNameTooLong,
    )?;
    if binding.pattern_type < 3 {
        return Err(CreateAclsPlanError::InvalidPatternType);
    }
    validate_string(
        &binding.principal,
        CreateAclsPlanError::EmptyPrincipal,
        CreateAclsPlanError::PrincipalTooLong,
    )?;
    validate_string(
        &binding.host,
        CreateAclsPlanError::EmptyHost,
        CreateAclsPlanError::HostTooLong,
    )?;
    if binding.operation < 2 {
        return Err(CreateAclsPlanError::InvalidOperation);
    }
    if binding.permission_type < 2 {
        return Err(CreateAclsPlanError::InvalidPermissionType);
    }
    Ok(())
}

fn validate_string(
    value: &str,
    empty: CreateAclsPlanError,
    too_long: CreateAclsPlanError,
) -> Result<(), CreateAclsPlanError> {
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > MAX_ACL_STRING_BYTES {
        return Err(too_long);
    }
    Ok(())
}

/// Invalid deterministic ACL-creation intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAclsPlanError {
    /// At least one concrete binding is required.
    EmptyBatch,
    /// The batch exceeds the fixed deterministic binding-count limit.
    BatchTooLarge,
    /// Resource types must be concrete rather than unknown or filter-only.
    InvalidResourceType,
    /// Concrete resource names must not be empty.
    EmptyResourceName,
    /// Resource names must fit Kafka's string domain.
    ResourceNameTooLong,
    /// Pattern types must be concrete rather than filter-only.
    InvalidPatternType,
    /// Principals must not be empty.
    EmptyPrincipal,
    /// Principals must fit Kafka's string domain.
    PrincipalTooLong,
    /// Hosts must not be empty.
    EmptyHost,
    /// Hosts must fit Kafka's string domain.
    HostTooLong,
    /// Operations must be concrete rather than unknown or filter-only.
    InvalidOperation,
    /// Permission types must be concrete rather than unknown or filter-only.
    InvalidPermissionType,
    /// One request cannot create the exact same binding twice.
    DuplicateBinding,
}

impl fmt::Display for CreateAclsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid CreateAcls plan: {self:?}")
    }
}

impl std::error::Error for CreateAclsPlanError {}
