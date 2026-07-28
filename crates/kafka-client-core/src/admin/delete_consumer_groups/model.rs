//! Validated caller-ordered intent for one Admin `DeleteConsumerGroups` operation.

use core::fmt;
use std::collections::BTreeSet;

const MAX_GROUP_ID_BYTES: usize = i16::MAX as usize;

/// One consumer group selected for deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupsTarget {
    group_id: String,
}

impl DeleteConsumerGroupsTarget {
    /// Creates one inert target for validation by the enclosing request plan.
    pub const fn new(group_id: String) -> Self {
        Self { group_id }
    }

    /// Returns the exact consumer-group identifier.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }
}

/// Validated intent for one bounded Admin `DeleteConsumerGroups` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupsPlan {
    targets: Vec<DeleteConsumerGroupsTarget>,
}

impl DeleteConsumerGroupsPlan {
    /// Validates one nonempty caller-ordered set of unique consumer groups.
    pub fn new(
        targets: Vec<DeleteConsumerGroupsTarget>,
    ) -> Result<Self, DeleteConsumerGroupsPlanError> {
        if targets.is_empty() {
            return Err(DeleteConsumerGroupsPlanError::EmptyTargetBatch);
        }
        let mut identities = BTreeSet::new();
        for target in &targets {
            if target.group_id.is_empty() {
                return Err(DeleteConsumerGroupsPlanError::EmptyGroupId);
            }
            if target.group_id.len() > MAX_GROUP_ID_BYTES {
                return Err(DeleteConsumerGroupsPlanError::GroupIdTooLong);
            }
            if !identities.insert(target.group_id.as_str()) {
                return Err(DeleteConsumerGroupsPlanError::DuplicateGroupId);
            }
        }
        Ok(Self { targets })
    }

    /// Returns consumer-group targets in exact caller order.
    pub fn targets(&self) -> &[DeleteConsumerGroupsTarget] {
        &self.targets
    }
}

/// Invalid deterministic Admin `DeleteConsumerGroups` intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteConsumerGroupsPlanError {
    /// An operation must contain at least one consumer group.
    EmptyTargetBatch,
    /// Consumer-group identifiers must not be empty.
    EmptyGroupId,
    /// A consumer-group identifier cannot fit Kafka's string domain.
    GroupIdTooLong,
    /// One operation cannot repeat a consumer-group identifier.
    DuplicateGroupId,
}

impl fmt::Display for DeleteConsumerGroupsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyTargetBatch => "Admin DeleteConsumerGroups target batch is empty",
            Self::EmptyGroupId => "Admin DeleteConsumerGroups group id is empty",
            Self::GroupIdTooLong => "Admin DeleteConsumerGroups group id is too long",
            Self::DuplicateGroupId => "Admin DeleteConsumerGroups contains a duplicate group id",
        })
    }
}

impl std::error::Error for DeleteConsumerGroupsPlanError {}
