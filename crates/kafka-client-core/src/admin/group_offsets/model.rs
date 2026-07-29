//! Validated semantic input for one or more consumer-group offset queries.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum UTF-8 byte length accepted for one group coordinator key.
pub(super) const MAX_CONSUMER_GROUP_ID_BYTES: usize = i16::MAX as usize;
/// Maximum group identities retained by one accepted batch operation.
pub(super) const MAX_CONSUMER_GROUPS: usize = 16 * 1024;
/// Maximum aggregate group-identity text retained by one batch operation.
pub(super) const MAX_CONSUMER_GROUP_REQUEST_TEXT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ListConsumerGroupOffsetsSelection {
    Singular(String),
    Batch(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListConsumerGroupOffsetsPlanShape {
    Singular,
    Batch,
}

/// Validated intent for one accepted all-partition offset operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetsPlan {
    selection: ListConsumerGroupOffsetsSelection,
    require_stable: bool,
}

impl ListConsumerGroupOffsetsPlan {
    /// Validates one explicit group identity and its stability requirement.
    pub fn new(
        group_id: String,
        require_stable: bool,
    ) -> Result<Self, ListConsumerGroupOffsetsPlanError> {
        validate_group_ids(core::slice::from_ref(&group_id))?;
        Ok(Self {
            selection: ListConsumerGroupOffsetsSelection::Singular(group_id),
            require_stable,
        })
    }

    /// Validates a nonempty, unique, caller-ordered consumer-group batch.
    pub fn new_batch(
        group_ids: Vec<String>,
        require_stable: bool,
    ) -> Result<Self, ListConsumerGroupOffsetsPlanError> {
        if group_ids.is_empty() {
            return Err(ListConsumerGroupOffsetsPlanError::EmptyGroupBatch);
        }
        validate_group_ids(&group_ids)?;
        Ok(Self {
            selection: ListConsumerGroupOffsetsSelection::Batch(group_ids),
            require_stable,
        })
    }

    /// Returns the first exact UTF-8 group identity.
    ///
    /// Every emitted `Submit` effect carries a one-group projection, preserving
    /// this accessor for the existing coordinator-routed protocol seam.
    pub fn group_id(&self) -> &str {
        match &self.selection {
            ListConsumerGroupOffsetsSelection::Singular(group_id) => group_id,
            ListConsumerGroupOffsetsSelection::Batch(group_ids) => &group_ids[0],
        }
    }

    /// Returns exact group identities in caller order.
    pub fn group_ids(&self) -> &[String] {
        match &self.selection {
            ListConsumerGroupOffsetsSelection::Singular(group_id) => {
                core::slice::from_ref(group_id)
            }
            ListConsumerGroupOffsetsSelection::Batch(group_ids) => group_ids,
        }
    }

    /// Returns whether Kafka must reject unstable group state.
    pub const fn require_stable(&self) -> bool {
        self.require_stable
    }

    pub(crate) fn shape(&self) -> ListConsumerGroupOffsetsPlanShape {
        match &self.selection {
            ListConsumerGroupOffsetsSelection::Singular(_) => {
                ListConsumerGroupOffsetsPlanShape::Singular
            }
            ListConsumerGroupOffsetsSelection::Batch(_) => ListConsumerGroupOffsetsPlanShape::Batch,
        }
    }

    pub(crate) fn singleton_at(&self, index: usize) -> Option<Self> {
        self.group_ids().get(index).cloned().map(|group_id| Self {
            selection: ListConsumerGroupOffsetsSelection::Singular(group_id),
            require_stable: self.require_stable,
        })
    }
}

/// Invalid deterministic group-offset query intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListConsumerGroupOffsetsPlanError {
    /// A batch operation must name at least one consumer group.
    EmptyGroupBatch,
    /// The query must name one explicit consumer group.
    EmptyGroupId,
    /// The UTF-8 group identity cannot fit the coordinator key domain.
    GroupIdTooLong,
    /// One accepted operation cannot retain more than the bounded group count.
    TooManyGroups,
    /// A batch operation cannot repeat a consumer-group identity.
    DuplicateGroupId,
    /// Aggregate group-identity text exceeds the semantic bound.
    RequestTextTooLarge,
}

impl fmt::Display for ListConsumerGroupOffsetsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyGroupBatch => "consumer group batch is empty",
            Self::EmptyGroupId => "consumer group id is empty",
            Self::GroupIdTooLong => "consumer group id exceeds the coordinator key limit",
            Self::TooManyGroups => "consumer group batch exceeds the group-count limit",
            Self::DuplicateGroupId => "consumer group batch contains a duplicate group id",
            Self::RequestTextTooLarge => {
                "consumer group batch exceeds the aggregate group-id byte limit"
            }
        })
    }
}

impl std::error::Error for ListConsumerGroupOffsetsPlanError {}

fn validate_group_ids(group_ids: &[String]) -> Result<(), ListConsumerGroupOffsetsPlanError> {
    if group_ids.len() > MAX_CONSUMER_GROUPS {
        return Err(ListConsumerGroupOffsetsPlanError::TooManyGroups);
    }
    let mut request_text_bytes = 0usize;
    let mut unique = BTreeSet::new();
    for group_id in group_ids {
        if group_id.is_empty() {
            return Err(ListConsumerGroupOffsetsPlanError::EmptyGroupId);
        }
        if group_id.len() > MAX_CONSUMER_GROUP_ID_BYTES {
            return Err(ListConsumerGroupOffsetsPlanError::GroupIdTooLong);
        }
        request_text_bytes = request_text_bytes
            .checked_add(group_id.len())
            .ok_or(ListConsumerGroupOffsetsPlanError::RequestTextTooLarge)?;
        if request_text_bytes > MAX_CONSUMER_GROUP_REQUEST_TEXT_BYTES {
            return Err(ListConsumerGroupOffsetsPlanError::RequestTextTooLarge);
        }
        if !unique.insert(group_id.as_str()) {
            return Err(ListConsumerGroupOffsetsPlanError::DuplicateGroupId);
        }
    }
    Ok(())
}
