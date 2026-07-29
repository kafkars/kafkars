//! Validated caller-ordered input for one or more API-77 share-group descriptions.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum UTF-8 bytes in one share-group coordinator identity.
pub const DESCRIBE_SHARE_GROUP_MAX_GROUP_ID_BYTES: usize = i16::MAX as usize;
/// Maximum aggregate request text retained by one operation.
pub const DESCRIBE_SHARE_GROUP_MAX_REQUEST_TEXT_BYTES: usize = 1024 * 1024;
/// Maximum share groups retained by one operation.
pub const DESCRIBE_SHARE_GROUP_MAX_GROUPS: usize = 16 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
enum DescribeShareGroupSelection {
    Singular(String),
    Batch(Vec<String>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeShareGroupPlanShape {
    Singular,
    Batch,
}

/// Validated intent for caller-ordered read-only stable-v1 API-77 requests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupPlan {
    selection: DescribeShareGroupSelection,
    include_authorized_operations: bool,
}

impl DescribeShareGroupPlan {
    /// Validates one explicit group for the existing singular operation.
    pub fn new(
        group_id: String,
        include_authorized_operations: bool,
    ) -> Result<Self, DescribeShareGroupPlanError> {
        validate_group_ids(core::slice::from_ref(&group_id))?;
        Ok(Self {
            selection: DescribeShareGroupSelection::Singular(group_id),
            include_authorized_operations,
        })
    }

    /// Validates a nonempty, unique, caller-ordered group batch.
    pub fn new_batch(
        group_ids: Vec<String>,
        include_authorized_operations: bool,
    ) -> Result<Self, DescribeShareGroupPlanError> {
        if group_ids.is_empty() {
            return Err(DescribeShareGroupPlanError::EmptyGroupBatch);
        }
        validate_group_ids(&group_ids)?;
        Ok(Self {
            selection: DescribeShareGroupSelection::Batch(group_ids),
            include_authorized_operations,
        })
    }

    /// Returns the first group-coordinator key.
    ///
    /// Every emitted `Submit` effect carries a one-element projection, which
    /// preserves this accessor for the existing engine protocol seam.
    pub fn group_id(&self) -> &str {
        match &self.selection {
            DescribeShareGroupSelection::Singular(group_id) => group_id,
            DescribeShareGroupSelection::Batch(group_ids) => &group_ids[0],
        }
    }

    /// Returns group coordinator keys in exact caller order.
    pub fn group_ids(&self) -> &[String] {
        match &self.selection {
            DescribeShareGroupSelection::Singular(group_id) => core::slice::from_ref(group_id),
            DescribeShareGroupSelection::Batch(group_ids) => group_ids,
        }
    }

    /// Reports whether Kafka authorization bits were requested.
    pub const fn include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
    }

    pub(crate) fn shape(&self) -> DescribeShareGroupPlanShape {
        match &self.selection {
            DescribeShareGroupSelection::Singular(_) => DescribeShareGroupPlanShape::Singular,
            DescribeShareGroupSelection::Batch(_) => DescribeShareGroupPlanShape::Batch,
        }
    }

    pub(crate) fn singleton_at(&self, index: usize) -> Option<Self> {
        self.group_ids().get(index).cloned().map(|group_id| Self {
            selection: DescribeShareGroupSelection::Singular(group_id),
            include_authorized_operations: self.include_authorized_operations,
        })
    }
}

fn validate_group_ids(group_ids: &[String]) -> Result<(), DescribeShareGroupPlanError> {
    if group_ids.len() > DESCRIBE_SHARE_GROUP_MAX_GROUPS {
        return Err(DescribeShareGroupPlanError::TooManyGroups);
    }
    let mut request_text_bytes = 0usize;
    let mut unique = BTreeSet::new();
    for group_id in group_ids {
        if group_id.is_empty() {
            return Err(DescribeShareGroupPlanError::EmptyGroupId);
        }
        if group_id.len() > DESCRIBE_SHARE_GROUP_MAX_GROUP_ID_BYTES {
            return Err(DescribeShareGroupPlanError::GroupIdTooLong);
        }
        request_text_bytes = request_text_bytes
            .checked_add(group_id.len())
            .ok_or(DescribeShareGroupPlanError::RequestTextTooLarge)?;
        if request_text_bytes > DESCRIBE_SHARE_GROUP_MAX_REQUEST_TEXT_BYTES {
            return Err(DescribeShareGroupPlanError::RequestTextTooLarge);
        }
        if !unique.insert(group_id.as_str()) {
            return Err(DescribeShareGroupPlanError::DuplicateGroupId);
        }
    }
    Ok(())
}

/// Invalid deterministic caller-ordered API-77 request intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupPlanError {
    /// A batch operation must name at least one share group.
    EmptyGroupBatch,
    /// Every request must name a nonempty share group.
    EmptyGroupId,
    /// The group identity cannot fit Kafka's string domain.
    GroupIdTooLong,
    /// One operation cannot retain more than the bounded group count.
    TooManyGroups,
    /// One operation cannot repeat a share-group identity.
    DuplicateGroupId,
    /// Aggregate request text exceeds the semantic bound.
    RequestTextTooLarge,
}

impl fmt::Display for DescribeShareGroupPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeShareGroup plan: {self:?}")
    }
}

impl std::error::Error for DescribeShareGroupPlanError {}
