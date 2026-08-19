//! Validated semantic input for one ordered batched `DescribeConfigs` operation.

use core::fmt;
use std::collections::BTreeSet;

/// Driver-independent destination for one `DescribeConfigs` subplan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DescribeConfigsRoute {
    /// Submit resources whose Kafka semantics do not name an exact broker.
    AnyBroker,
    /// Submit broker and broker-logger resources to their canonical broker ID.
    ExactBroker(i32),
}

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

    pub(crate) fn route(&self) -> DescribeConfigsRoute {
        route_for(self)
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
            if matches!(resource.resource_type, 4 | 8)
                && canonical_broker_id(&resource.resource_name).is_none()
            {
                return Err(DescribeConfigsPlanError::InvalidBrokerResourceName);
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

    pub(crate) fn route_order(&self) -> Vec<DescribeConfigsRoute> {
        let mut routes = Vec::new();
        let mut seen = BTreeSet::new();
        for resource in &self.resources {
            let route = resource.route();
            if seen.insert(route) {
                routes.push(route);
            }
        }
        routes
    }

    pub(crate) fn subplan(&self, route: DescribeConfigsRoute) -> Self {
        Self {
            resources: self
                .resources
                .iter()
                .filter(|resource| resource.route() == route)
                .cloned()
                .collect(),
            include_synonyms: self.include_synonyms,
            include_documentation: self.include_documentation,
        }
    }
}

fn route_for(resource: &DescribeConfigsResourceQuery) -> DescribeConfigsRoute {
    if matches!(resource.resource_type, 4 | 8) {
        let Some(broker_id) = canonical_broker_id(&resource.resource_name) else {
            unreachable!("validated broker resource name must remain canonical")
        };
        return DescribeConfigsRoute::ExactBroker(broker_id);
    }
    DescribeConfigsRoute::AnyBroker
}

fn canonical_broker_id(resource_name: &str) -> Option<i32> {
    let bytes = resource_name.as_bytes();
    if bytes == b"0" {
        return Some(0);
    }
    if !matches!(bytes.first(), Some(b'1'..=b'9'))
        || bytes.get(1..)?.iter().any(|byte| !byte.is_ascii_digit())
    {
        return None;
    }
    resource_name.parse().ok()
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
    /// Broker and broker-logger names must be canonical nonnegative `i32` IDs.
    InvalidBrokerResourceName,
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
