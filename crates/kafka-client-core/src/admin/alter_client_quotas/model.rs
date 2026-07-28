//! Validated wire-free intent for one caller-ordered client-quota alteration.

use core::fmt;
use std::collections::BTreeSet;

use super::{
    entity::AlterClientQuotaEntity,
    operation::{AlterClientQuotaOperation, AlterClientQuotaOperationKind},
};

/// Maximum entities accepted by one client-quota alteration.
pub const ALTER_CLIENT_QUOTAS_MAX_ENTRIES: usize = 1024;
/// Maximum type/name components in one client-quota entity identity.
pub const ALTER_CLIENT_QUOTAS_MAX_COMPONENTS_PER_ENTITY: usize = 128;
/// Maximum quota operations attached to one entity.
pub const ALTER_CLIENT_QUOTAS_MAX_OPERATIONS_PER_ENTITY: usize = 128;
/// Maximum UTF-8 bytes in an entity type, entity name, or quota key.
pub const ALTER_CLIENT_QUOTAS_MAX_STRING_BYTES: usize = 256;

/// One entity and its nonempty caller-ordered quota changes.
#[derive(Clone, Debug, PartialEq)]
pub struct AlterClientQuotaEntry {
    entity: AlterClientQuotaEntity,
    operations: Vec<AlterClientQuotaOperation>,
}

impl AlterClientQuotaEntry {
    /// Creates inert entry data for validation by the enclosing plan.
    pub const fn new(
        entity: AlterClientQuotaEntity,
        operations: Vec<AlterClientQuotaOperation>,
    ) -> Self {
        Self { entity, operations }
    }

    /// Returns the canonical entity identity after plan validation.
    pub const fn entity(&self) -> &AlterClientQuotaEntity {
        &self.entity
    }

    /// Returns quota operations in exact caller order.
    pub fn operations(&self) -> &[AlterClientQuotaOperation] {
        &self.operations
    }

    /// Consumes this entry into adapter-owned parts.
    pub fn into_parts(self) -> (AlterClientQuotaEntity, Vec<AlterClientQuotaOperation>) {
        (self.entity, self.operations)
    }
}

/// Validated intent for one destructive client-quota RPC.
#[derive(Clone, Debug, PartialEq)]
pub struct AlterClientQuotasPlan {
    entries: Vec<AlterClientQuotaEntry>,
    validate_only: bool,
}

impl AlterClientQuotasPlan {
    /// Validates bounds and identities while retaining caller entry/operation order.
    pub fn new(
        mut entries: Vec<AlterClientQuotaEntry>,
        validate_only: bool,
    ) -> Result<Self, AlterClientQuotasPlanError> {
        if entries.is_empty() {
            return Err(AlterClientQuotasPlanError::EmptyBatch);
        }
        if entries.len() > ALTER_CLIENT_QUOTAS_MAX_ENTRIES {
            return Err(AlterClientQuotasPlanError::TooManyEntries);
        }
        let mut identities = BTreeSet::new();
        for entry in &mut entries {
            entry.entity.validate_and_canonicalize()?;
            validate_operations(&entry.operations)?;
            if !identities.insert(entry.entity.clone()) {
                return Err(AlterClientQuotasPlanError::DuplicateEntity);
            }
        }
        Ok(Self {
            entries,
            validate_only,
        })
    }

    /// Returns entries in exact caller order with canonical entity identities.
    pub fn entries(&self) -> &[AlterClientQuotaEntry] {
        &self.entries
    }

    /// Returns whether Kafka should validate without mutating.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }
}

fn validate_operations(
    operations: &[AlterClientQuotaOperation],
) -> Result<(), AlterClientQuotasPlanError> {
    if operations.is_empty() {
        return Err(AlterClientQuotasPlanError::EmptyOperations);
    }
    if operations.len() > ALTER_CLIENT_QUOTAS_MAX_OPERATIONS_PER_ENTITY {
        return Err(AlterClientQuotasPlanError::TooManyOperations);
    }
    let mut keys = BTreeSet::new();
    for operation in operations {
        validate_string(
            operation.key(),
            AlterClientQuotasPlanError::EmptyQuotaKey,
            AlterClientQuotasPlanError::QuotaKeyTooLong,
        )?;
        if !keys.insert(operation.key()) {
            return Err(AlterClientQuotasPlanError::DuplicateQuotaKey);
        }
        if matches!(operation.kind(), AlterClientQuotaOperationKind::Set(value) if !value.is_finite())
        {
            return Err(AlterClientQuotasPlanError::NonFiniteQuotaValue);
        }
    }
    Ok(())
}

fn validate_string(
    value: &str,
    empty: AlterClientQuotasPlanError,
    too_long: AlterClientQuotasPlanError,
) -> Result<(), AlterClientQuotasPlanError> {
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > ALTER_CLIENT_QUOTAS_MAX_STRING_BYTES {
        return Err(too_long);
    }
    Ok(())
}

/// Invalid deterministic client-quota alteration intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasPlanError {
    /// Kafka cannot execute an empty entity batch.
    EmptyBatch,
    /// One operation cannot retain more than 1024 entities.
    TooManyEntries,
    /// Every entry requires a concrete entity identity.
    EmptyEntity,
    /// One entity cannot retain more than 128 identity components.
    TooManyEntityComponents,
    /// Entity types must not be empty.
    EmptyEntityType,
    /// Entity types cannot exceed 256 UTF-8 bytes.
    EntityTypeTooLong,
    /// Present entity names must not be empty.
    EmptyEntityName,
    /// Entity names cannot exceed 256 UTF-8 bytes.
    EntityNameTooLong,
    /// One entity cannot repeat an entity type.
    DuplicateEntityType,
    /// A batch cannot repeat a canonical entity identity.
    DuplicateEntity,
    /// Every entity requires at least one quota operation.
    EmptyOperations,
    /// One entity cannot retain more than 128 quota operations.
    TooManyOperations,
    /// Quota keys must not be empty.
    EmptyQuotaKey,
    /// Quota keys cannot exceed 256 UTF-8 bytes.
    QuotaKeyTooLong,
    /// One entity cannot alter the same key twice.
    DuplicateQuotaKey,
    /// Assigned quota values must be finite.
    NonFiniteQuotaValue,
}

impl fmt::Display for AlterClientQuotasPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid AlterClientQuotas plan: {self:?}")
    }
}

impl std::error::Error for AlterClientQuotasPlanError {}
