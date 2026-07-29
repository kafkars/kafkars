//! Validated caller-ordered intent for one `DescribeGroups` operation.

use core::fmt;
use std::collections::BTreeSet;

const MAX_GROUP_ID_BYTES: usize = i16::MAX as usize;

/// Explicit protocol-family policy for consumer-group description.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeConsumerGroupsScope {
    /// Attempt KIP-848 first and permit one explicit classic fallback.
    ModernFirst,
    /// Use classic `DescribeGroups` directly without a modern attempt.
    ClassicOnly,
}

/// Validated bounded intent for describing classic consumer groups.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeConsumerGroupsPlan {
    groups: Vec<String>,
    include_authorized_operations: bool,
    scope: AdminDescribeConsumerGroupsScope,
}

impl AdminDescribeConsumerGroupsPlan {
    /// Validates a modern-first, caller-ordered set of unique group IDs.
    pub fn new(
        groups: Vec<String>,
        include_authorized_operations: bool,
    ) -> Result<Self, AdminDescribeConsumerGroupsPlanError> {
        Self::with_scope(
            groups,
            include_authorized_operations,
            AdminDescribeConsumerGroupsScope::ModernFirst,
        )
    }

    /// Validates a caller-ordered set under one explicit protocol-family scope.
    pub fn with_scope(
        groups: Vec<String>,
        include_authorized_operations: bool,
        scope: AdminDescribeConsumerGroupsScope,
    ) -> Result<Self, AdminDescribeConsumerGroupsPlanError> {
        if groups.is_empty() {
            return Err(AdminDescribeConsumerGroupsPlanError::EmptyGroupBatch);
        }
        let mut identities = BTreeSet::new();
        for group in &groups {
            if group.is_empty() {
                return Err(AdminDescribeConsumerGroupsPlanError::EmptyGroupId);
            }
            if group.len() > MAX_GROUP_ID_BYTES {
                return Err(AdminDescribeConsumerGroupsPlanError::GroupIdTooLong);
            }
            if !identities.insert(group.as_str()) {
                return Err(AdminDescribeConsumerGroupsPlanError::DuplicateGroupId);
            }
        }
        Ok(Self {
            groups,
            include_authorized_operations,
            scope,
        })
    }

    /// Returns group IDs in exact caller order.
    pub fn groups(&self) -> &[String] {
        &self.groups
    }

    /// Returns whether Kafka authorization bits were explicitly requested.
    pub const fn include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
    }

    /// Returns the immutable protocol-family scope.
    pub const fn scope(&self) -> AdminDescribeConsumerGroupsScope {
        self.scope
    }
}

/// Invalid deterministic consumer-group description intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeConsumerGroupsPlanError {
    /// At least one group must be requested.
    EmptyGroupBatch,
    /// Group IDs must not be empty.
    EmptyGroupId,
    /// A group ID exceeds Kafka's coordinator-key string domain.
    GroupIdTooLong,
    /// One operation cannot repeat a group ID.
    DuplicateGroupId,
}

impl fmt::Display for AdminDescribeConsumerGroupsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyGroupBatch => "DescribeConsumerGroups group batch is empty",
            Self::EmptyGroupId => "DescribeConsumerGroups group ID is empty",
            Self::GroupIdTooLong => "DescribeConsumerGroups group ID is too long",
            Self::DuplicateGroupId => "DescribeConsumerGroups contains a duplicate group ID",
        })
    }
}

impl std::error::Error for AdminDescribeConsumerGroupsPlanError {}
