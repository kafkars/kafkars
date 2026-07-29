//! Validated semantic input for ordered configuration-resource alterations.

use core::fmt;

mod resource;

use resource::validate_resources;
pub use resource::{
    ConfigAlteration, ConfigAlterationOperation, IncrementalConfigResourceAlteration,
    TopicConfigAlteration,
};

/// Ordered, validated policy input for one incremental configuration RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsPlan {
    resources: Vec<IncrementalConfigResourceAlteration>,
    validate_only: bool,
}

impl IncrementalAlterConfigsPlan {
    /// Validates nonempty topics and unambiguous alteration identities.
    pub fn new(
        topics: Vec<TopicConfigAlteration>,
        validate_only: bool,
    ) -> Result<Self, IncrementalAlterConfigsPlanError> {
        validate_resources(&topics, true)?;
        Ok(Self {
            resources: topics,
            validate_only,
        })
    }

    /// Validates exact positive resource identities and unambiguous changes.
    pub fn for_resources(
        resources: Vec<IncrementalConfigResourceAlteration>,
        validate_only: bool,
    ) -> Result<Self, IncrementalAlterConfigsPlanError> {
        validate_resources(&resources, false)?;
        Ok(Self {
            resources,
            validate_only,
        })
    }

    /// Returns resources in original caller order.
    pub fn resources(&self) -> &[IncrementalConfigResourceAlteration] {
        &self.resources
    }

    /// Returns topic-compatible values in original caller order.
    ///
    /// Existing topic-only callers continue to receive the same values.
    pub fn topics(&self) -> &[TopicConfigAlteration] {
        &self.resources
    }

    /// Returns whether Kafka should validate without mutating.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }
}

/// Invalid deterministic topic configuration input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalAlterConfigsPlanError {
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
    /// One exact resource type and name may occur only once in a batch.
    DuplicateResource,
    /// Every resource must contain at least one alteration.
    EmptyAlterations,
    /// Configuration keys must not be empty.
    EmptyConfigurationKey,
    /// One key may occur only once for a topic.
    DuplicateConfigurationKey,
}

impl fmt::Display for IncrementalAlterConfigsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid IncrementalAlterConfigs plan: {self:?}")
    }
}

impl std::error::Error for IncrementalAlterConfigsPlanError {}
