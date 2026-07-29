//! Validated caller-ordered input for one or more API-89 streams-group descriptions.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum UTF-8 bytes in one streams-group coordinator identity.
pub const DESCRIBE_STREAMS_GROUP_MAX_GROUP_ID_BYTES: usize = i16::MAX as usize;
/// Maximum aggregate request text retained by one operation.
pub const DESCRIBE_STREAMS_GROUP_MAX_REQUEST_TEXT_BYTES: usize = 1024 * 1024;
/// Maximum streams groups retained by one operation.
pub const DESCRIBE_STREAMS_GROUP_MAX_GROUPS: usize = 16 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
enum DescribeStreamsGroupSelection {
    Singular(String),
    Batch(Vec<String>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeStreamsGroupPlanShape {
    Singular,
    Batch,
}

/// Validated intent for caller-ordered read-only API-89 requests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupPlan {
    selection: DescribeStreamsGroupSelection,
    include_authorized_operations: bool,
    include_topology_description: bool,
}

impl DescribeStreamsGroupPlan {
    /// Validates one explicit group for the existing singular operation.
    pub fn new(
        group_id: String,
        include_authorized_operations: bool,
        include_topology_description: bool,
    ) -> Result<Self, DescribeStreamsGroupPlanError> {
        validate_group_ids(core::slice::from_ref(&group_id))?;
        Ok(Self {
            selection: DescribeStreamsGroupSelection::Singular(group_id),
            include_authorized_operations,
            include_topology_description,
        })
    }

    /// Validates a nonempty, unique, caller-ordered group batch.
    pub fn new_batch(
        group_ids: Vec<String>,
        include_authorized_operations: bool,
        include_topology_description: bool,
    ) -> Result<Self, DescribeStreamsGroupPlanError> {
        if group_ids.is_empty() {
            return Err(DescribeStreamsGroupPlanError::EmptyGroupBatch);
        }
        validate_group_ids(&group_ids)?;
        Ok(Self {
            selection: DescribeStreamsGroupSelection::Batch(group_ids),
            include_authorized_operations,
            include_topology_description,
        })
    }

    /// Returns the first group-coordinator key.
    ///
    /// Every emitted `Submit` effect carries a one-element projection, which
    /// preserves this accessor for the existing engine protocol seam.
    pub fn group_id(&self) -> &str {
        match &self.selection {
            DescribeStreamsGroupSelection::Singular(group_id) => group_id,
            DescribeStreamsGroupSelection::Batch(group_ids) => &group_ids[0],
        }
    }

    /// Returns group coordinator keys in exact caller order.
    pub fn group_ids(&self) -> &[String] {
        match &self.selection {
            DescribeStreamsGroupSelection::Singular(group_id) => core::slice::from_ref(group_id),
            DescribeStreamsGroupSelection::Batch(group_ids) => group_ids,
        }
    }

    /// Reports whether Kafka authorization bits were requested.
    pub const fn include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
    }

    /// Reports whether stable v1 topology-description expansion was requested.
    pub const fn include_topology_description(&self) -> bool {
        self.include_topology_description
    }

    pub(crate) fn shape(&self) -> DescribeStreamsGroupPlanShape {
        match &self.selection {
            DescribeStreamsGroupSelection::Singular(_) => DescribeStreamsGroupPlanShape::Singular,
            DescribeStreamsGroupSelection::Batch(_) => DescribeStreamsGroupPlanShape::Batch,
        }
    }

    pub(crate) fn singleton_at(&self, index: usize) -> Option<Self> {
        self.group_ids().get(index).cloned().map(|group_id| Self {
            selection: DescribeStreamsGroupSelection::Singular(group_id),
            include_authorized_operations: self.include_authorized_operations,
            include_topology_description: self.include_topology_description,
        })
    }

    /// Returns the oldest API version that can represent this exact plan.
    pub const fn minimum_version(&self) -> i16 {
        if self.include_topology_description {
            1
        } else {
            0
        }
    }
}

fn validate_group_ids(group_ids: &[String]) -> Result<(), DescribeStreamsGroupPlanError> {
    if group_ids.len() > DESCRIBE_STREAMS_GROUP_MAX_GROUPS {
        return Err(DescribeStreamsGroupPlanError::TooManyGroups);
    }
    let mut request_text_bytes = 0usize;
    let mut unique = BTreeSet::new();
    for group_id in group_ids {
        if group_id.is_empty() {
            return Err(DescribeStreamsGroupPlanError::EmptyGroupId);
        }
        if group_id.len() > DESCRIBE_STREAMS_GROUP_MAX_GROUP_ID_BYTES {
            return Err(DescribeStreamsGroupPlanError::GroupIdTooLong);
        }
        request_text_bytes = request_text_bytes
            .checked_add(group_id.len())
            .ok_or(DescribeStreamsGroupPlanError::RequestTextTooLarge)?;
        if request_text_bytes > DESCRIBE_STREAMS_GROUP_MAX_REQUEST_TEXT_BYTES {
            return Err(DescribeStreamsGroupPlanError::RequestTextTooLarge);
        }
        if !unique.insert(group_id.as_str()) {
            return Err(DescribeStreamsGroupPlanError::DuplicateGroupId);
        }
    }
    Ok(())
}

/// Invalid deterministic API-89 request intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupPlanError {
    /// A batch operation must name at least one streams group.
    EmptyGroupBatch,
    /// Every request must name one explicit streams group.
    EmptyGroupId,
    /// The group identity cannot fit Kafka's string domain.
    GroupIdTooLong,
    /// One operation cannot retain more than the bounded group count.
    TooManyGroups,
    /// One operation cannot repeat a streams-group identity.
    DuplicateGroupId,
    /// Aggregate request text exceeds the semantic bound.
    RequestTextTooLarge,
}

impl fmt::Display for DescribeStreamsGroupPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeStreamsGroup plan: {self:?}")
    }
}

impl std::error::Error for DescribeStreamsGroupPlanError {}
