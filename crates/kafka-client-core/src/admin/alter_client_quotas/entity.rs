//! Canonical type/name identity values for client-quota alteration.

use super::{
    ALTER_CLIENT_QUOTAS_MAX_COMPONENTS_PER_ENTITY, ALTER_CLIENT_QUOTAS_MAX_STRING_BYTES,
    AlterClientQuotasPlanError,
};

/// One type/name component of a client-quota entity identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AlterClientQuotaEntityComponent {
    entity_type: String,
    entity_name: Option<String>,
}

impl AlterClientQuotaEntityComponent {
    /// Creates inert component data for validation by a plan or response.
    pub const fn new(entity_type: String, entity_name: Option<String>) -> Self {
        Self {
            entity_type,
            entity_name,
        }
    }

    /// Returns the stable entity type.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns the concrete name, or `None` for the default entity.
    pub fn entity_name(&self) -> Option<&str> {
        self.entity_name.as_deref()
    }

    /// Consumes this component into adapter-owned scalar parts.
    pub fn into_parts(self) -> (String, Option<String>) {
        (self.entity_type, self.entity_name)
    }
}

/// One canonical client-quota entity identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AlterClientQuotaEntity {
    components: Vec<AlterClientQuotaEntityComponent>,
}

impl AlterClientQuotaEntity {
    /// Creates inert entity data for validation by a plan or response.
    pub const fn new(components: Vec<AlterClientQuotaEntityComponent>) -> Self {
        Self { components }
    }

    /// Returns components in canonical entity-type/name order after validation.
    pub fn components(&self) -> &[AlterClientQuotaEntityComponent] {
        &self.components
    }

    /// Consumes this identity into adapter-owned components.
    pub fn into_components(self) -> Vec<AlterClientQuotaEntityComponent> {
        self.components
    }

    pub(crate) fn validate_and_canonicalize(&mut self) -> Result<(), AlterClientQuotasPlanError> {
        if self.components.is_empty() {
            return Err(AlterClientQuotasPlanError::EmptyEntity);
        }
        if self.components.len() > ALTER_CLIENT_QUOTAS_MAX_COMPONENTS_PER_ENTITY {
            return Err(AlterClientQuotasPlanError::TooManyEntityComponents);
        }
        for component in &self.components {
            validate_string(
                component.entity_type(),
                AlterClientQuotasPlanError::EmptyEntityType,
                AlterClientQuotasPlanError::EntityTypeTooLong,
            )?;
            if let Some(name) = component.entity_name() {
                validate_string(
                    name,
                    AlterClientQuotasPlanError::EmptyEntityName,
                    AlterClientQuotasPlanError::EntityNameTooLong,
                )?;
            }
        }
        self.components.sort_unstable();
        if self
            .components
            .windows(2)
            .any(|pair| pair[0].entity_type() == pair[1].entity_type())
        {
            return Err(AlterClientQuotasPlanError::DuplicateEntityType);
        }
        Ok(())
    }
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
