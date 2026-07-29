//! Validated semantic input for explicit legacy configuration-resource replacement.

use core::fmt;
use std::collections::BTreeSet;

mod resource;

use resource::validate_resources;
pub use resource::{
    LegacyConfigEntry, LegacyConfigResourceReplacement, LegacyTopicConfigReplacement,
};

/// Driver-independent destination for one legacy configuration subplan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LegacyAlterConfigsRoute {
    /// Submit resources whose Kafka semantics do not name an exact broker.
    AnyBroker,
    /// Submit broker and broker-logger resources to their canonical broker ID.
    ExactBroker(i32),
}

/// Ordered input for one explicitly destructive API 33 operation.
///
/// Each resource's list is a complete dynamic-configuration snapshot. Kafka
/// implicitly resets omitted keys; this plan is never an incremental fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyAlterConfigsPlan {
    resources: Vec<LegacyConfigResourceReplacement>,
    validate_only: bool,
}

impl LegacyAlterConfigsPlan {
    /// Validates a nonempty unique-topic batch and unambiguous snapshots.
    pub fn new(
        topics: Vec<LegacyTopicConfigReplacement>,
        validate_only: bool,
    ) -> Result<Self, LegacyAlterConfigsPlanError> {
        validate_resources(&topics, true)?;
        Ok(Self {
            resources: topics,
            validate_only,
        })
    }

    /// Validates exact positive resource identities and unambiguous snapshots.
    pub fn for_resources(
        resources: Vec<LegacyConfigResourceReplacement>,
        validate_only: bool,
    ) -> Result<Self, LegacyAlterConfigsPlanError> {
        validate_resources(&resources, false)?;
        Ok(Self {
            resources,
            validate_only,
        })
    }

    /// Returns resources in original caller order.
    pub fn resources(&self) -> &[LegacyConfigResourceReplacement] {
        &self.resources
    }

    /// Returns topic snapshots in original caller order.
    ///
    /// Existing topic-only callers continue to receive the same values.
    pub fn topics(&self) -> &[LegacyTopicConfigReplacement] {
        &self.resources
    }

    /// Returns whether Kafka should validate without mutating.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }

    pub(crate) fn route_order(&self) -> Vec<LegacyAlterConfigsRoute> {
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

    pub(crate) fn subplan(&self, route: LegacyAlterConfigsRoute) -> Self {
        Self {
            resources: self
                .resources
                .iter()
                .filter(|resource| resource.route() == route)
                .cloned()
                .collect(),
            validate_only: self.validate_only,
        }
    }
}

/// Invalid deterministic legacy replacement input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyAlterConfigsPlanError {
    /// Kafka cannot execute an empty resource batch.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// One topic may occur only once in a batch.
    DuplicateTopic,
    /// Kafka configuration-resource type codes must be positive.
    NonPositiveResourceType,
    /// Generic Kafka resource names must not be empty.
    EmptyResourceName,
    /// Broker and broker-logger names must be canonical nonnegative `i32` IDs.
    InvalidBrokerResourceName,
    /// One exact resource type and name may occur only once in a batch.
    DuplicateResource,
    /// Configuration keys must not be empty.
    EmptyConfigurationKey,
    /// One key may occur only once in a topic snapshot.
    DuplicateConfigurationKey,
}

impl fmt::Display for LegacyAlterConfigsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid LegacyAlterConfigs plan: {self:?}")
    }
}

impl std::error::Error for LegacyAlterConfigsPlanError {}
