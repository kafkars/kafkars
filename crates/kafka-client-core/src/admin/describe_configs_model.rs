//! Validated semantic input for one ordered batched `DescribeConfigs` operation.

use core::fmt;
use std::collections::BTreeSet;

/// One resource and optional ordered configuration-key selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResourceQuery {
    resource_type: i8,
    resource_name: String,
    configuration_keys: Option<Vec<String>>,
}

impl DescribeConfigsResourceQuery {
    /// Creates one query for validation by [`DescribeConfigsPlan`].
    pub const fn new(
        resource_type: i8,
        resource_name: String,
        configuration_keys: Option<Vec<String>>,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            configuration_keys,
        }
    }

    /// Returns Kafka's positive configuration-resource type.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns `None` for all keys or the exact caller-ordered selection.
    pub fn configuration_keys(&self) -> Option<&[String]> {
        self.configuration_keys.as_deref()
    }
}

/// Ordered, validated policy input for one `DescribeConfigs` RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsPlan {
    resources: Vec<DescribeConfigsResourceQuery>,
    include_synonyms: bool,
    include_documentation: bool,
}

impl DescribeConfigsPlan {
    /// Validates one nonempty, unambiguous resource batch.
    pub fn new(
        resources: Vec<DescribeConfigsResourceQuery>,
        include_synonyms: bool,
        include_documentation: bool,
    ) -> Result<Self, DescribeConfigsPlanError> {
        if resources.is_empty() {
            return Err(DescribeConfigsPlanError::EmptyBatch);
        }
        let mut identities = BTreeSet::new();
        for resource in &resources {
            if resource.resource_type <= 0 {
                return Err(DescribeConfigsPlanError::InvalidResourceType);
            }
            if resource.resource_name.is_empty() {
                return Err(DescribeConfigsPlanError::EmptyResourceName);
            }
            if !identities.insert((resource.resource_type, resource.resource_name.as_str())) {
                return Err(DescribeConfigsPlanError::DuplicateResource);
            }
            validate_keys(resource.configuration_keys.as_deref())?;
        }
        Ok(Self {
            resources,
            include_synonyms,
            include_documentation,
        })
    }

    /// Returns resources in original caller order.
    pub fn resources(&self) -> &[DescribeConfigsResourceQuery] {
        &self.resources
    }

    /// Returns whether Kafka should include configuration synonyms.
    pub const fn include_synonyms(&self) -> bool {
        self.include_synonyms
    }

    /// Returns whether Kafka should include configuration documentation.
    pub const fn include_documentation(&self) -> bool {
        self.include_documentation
    }
}

fn validate_keys(keys: Option<&[String]>) -> Result<(), DescribeConfigsPlanError> {
    let Some(keys) = keys else {
        return Ok(());
    };
    let mut unique = BTreeSet::new();
    for key in keys {
        if key.is_empty() {
            return Err(DescribeConfigsPlanError::EmptyConfigurationKey);
        }
        if !unique.insert(key.as_str()) {
            return Err(DescribeConfigsPlanError::DuplicateConfigurationKey);
        }
    }
    Ok(())
}

/// Invalid deterministic `DescribeConfigs` input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeConfigsPlanError {
    /// Kafka cannot execute an empty resource batch.
    EmptyBatch,
    /// Resource type zero and negative values are not valid request types.
    InvalidResourceType,
    /// Resource names must not be empty.
    EmptyResourceName,
    /// One type/name identity may occur only once.
    DuplicateResource,
    /// Selected configuration keys must not be empty.
    EmptyConfigurationKey,
    /// Selected configuration keys must be unique within one resource.
    DuplicateConfigurationKey,
}

impl fmt::Display for DescribeConfigsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeConfigs plan: {self:?}")
    }
}

impl std::error::Error for DescribeConfigsPlanError {}
