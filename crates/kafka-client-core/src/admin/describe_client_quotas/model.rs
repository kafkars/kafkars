//! Validated wire-free filter intent for one client-quota description query.

use core::fmt;
use std::collections::BTreeSet;

const MAX_FILTER_COMPONENTS: usize = 128;
const MAX_FILTER_STRING_BYTES: usize = i16::MAX as usize;

/// Stable client-quota entity-name selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientQuotaMatch {
    /// Match one exact nonempty entity name.
    Exact(String),
    /// Match the unnamed default entity.
    Default,
    /// Match any explicitly named entity.
    AnySpecified,
}

/// One caller-ordered client-quota entity filter component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotaFilterComponent {
    entity_type: String,
    match_kind: ClientQuotaMatch,
}

impl DescribeClientQuotaFilterComponent {
    /// Creates inert filter intent for validation by the enclosing plan.
    pub const fn new(entity_type: String, match_kind: ClientQuotaMatch) -> Self {
        Self {
            entity_type,
            match_kind,
        }
    }

    /// Returns the stable entity-type identity.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns how this component selects entity names.
    pub const fn match_kind(&self) -> &ClientQuotaMatch {
        &self.match_kind
    }

    /// Consumes this component into adapter-owned stable parts.
    pub fn into_parts(self) -> (String, ClientQuotaMatch) {
        (self.entity_type, self.match_kind)
    }
}

/// Validated intent for one bounded client-quota description query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotasPlan {
    components: Vec<DescribeClientQuotaFilterComponent>,
    strict: bool,
}

impl DescribeClientQuotasPlan {
    /// Validates caller order, unique types, and version-zero string bounds.
    ///
    /// An empty component vector is Kafka's explicit all-entities filter.
    pub fn new(
        components: Vec<DescribeClientQuotaFilterComponent>,
        strict: bool,
    ) -> Result<Self, DescribeClientQuotasPlanError> {
        if components.len() > MAX_FILTER_COMPONENTS {
            return Err(DescribeClientQuotasPlanError::TooManyFilterComponents);
        }
        let mut entity_types = BTreeSet::new();
        for component in &components {
            validate_string(
                component.entity_type(),
                DescribeClientQuotasPlanError::EmptyEntityType,
                DescribeClientQuotasPlanError::EntityTypeTooLong,
            )?;
            if !entity_types.insert(component.entity_type()) {
                return Err(DescribeClientQuotasPlanError::DuplicateEntityType);
            }
            if let ClientQuotaMatch::Exact(entity_name) = component.match_kind() {
                validate_string(
                    entity_name,
                    DescribeClientQuotasPlanError::EmptyExactEntityName,
                    DescribeClientQuotasPlanError::ExactEntityNameTooLong,
                )?;
            }
        }
        Ok(Self { components, strict })
    }

    /// Returns filter components in exact caller order.
    pub fn components(&self) -> &[DescribeClientQuotaFilterComponent] {
        &self.components
    }

    /// Returns whether unmatched entity types must be excluded.
    pub const fn strict(&self) -> bool {
        self.strict
    }
}

fn validate_string(
    value: &str,
    empty: DescribeClientQuotasPlanError,
    too_long: DescribeClientQuotasPlanError,
) -> Result<(), DescribeClientQuotasPlanError> {
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > MAX_FILTER_STRING_BYTES {
        return Err(too_long);
    }
    Ok(())
}

/// Invalid deterministic client-quota filter intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClientQuotasPlanError {
    /// One query cannot retain more than 128 filter components.
    TooManyFilterComponents,
    /// Entity types must not be empty.
    EmptyEntityType,
    /// Entity types must fit the version-zero string domain.
    EntityTypeTooLong,
    /// One query cannot repeat an entity type.
    DuplicateEntityType,
    /// Exact selection requires a nonempty entity name.
    EmptyExactEntityName,
    /// Exact entity names must fit the version-zero string domain.
    ExactEntityNameTooLong,
}

impl fmt::Display for DescribeClientQuotasPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeClientQuotas filter: {self:?}")
    }
}

impl std::error::Error for DescribeClientQuotasPlanError {}
