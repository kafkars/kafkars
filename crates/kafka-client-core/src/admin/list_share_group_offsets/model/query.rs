//! One validated API-90 share-group selection and its bounded partition intent.

use std::collections::BTreeSet;

use super::ListShareGroupOffsetsPlanError;

/// Maximum UTF-8 bytes in one share-group coordinator identity.
pub const LIST_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES: usize = i16::MAX as usize;
/// Maximum UTF-8 bytes in one selected topic name.
pub const LIST_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;
/// Maximum caller-selected topic-partitions retained by one operation.
pub const LIST_SHARE_GROUP_OFFSETS_MAX_SELECTED_PARTITIONS: usize = 4 * 1024;
/// Maximum aggregate group and selected-topic bytes retained by one operation.
pub const LIST_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES: usize = 1024 * 1024;

/// One caller-ordered topic-partition selected for share-offset description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetTarget {
    topic: String,
    partition: i32,
}

impl ListShareGroupOffsetTarget {
    /// Creates one inert target for validation by the enclosing plan.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    /// Returns the exact UTF-8 topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }
}

/// Explicit API-90 query mode; an empty selection never means all partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsSelection {
    /// Every share-group topic-partition visible to the coordinator.
    All,
    /// One nonempty caller-ordered set of unique topic-partitions.
    Selected(Vec<ListShareGroupOffsetTarget>),
}

/// One validated share-group query and its exact all-or-selected partition intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsQuery {
    group_id: String,
    selection: ListShareGroupOffsetsSelection,
}

impl ListShareGroupOffsetsQuery {
    /// Validates one explicit group and explicit all-or-selected query mode.
    pub fn new(
        group_id: String,
        selection: ListShareGroupOffsetsSelection,
    ) -> Result<Self, ListShareGroupOffsetsPlanError> {
        validate_group(&group_id)?;
        if let ListShareGroupOffsetsSelection::Selected(targets) = &selection {
            validate_targets(group_id.len(), targets)?;
        }
        Ok(Self {
            group_id,
            selection,
        })
    }

    /// Validates one query for all share-group topic-partitions.
    pub fn all(group_id: String) -> Result<Self, ListShareGroupOffsetsPlanError> {
        Self::new(group_id, ListShareGroupOffsetsSelection::All)
    }

    /// Validates one nonempty caller-ordered topic-partition selection.
    pub fn selected(
        group_id: String,
        targets: Vec<ListShareGroupOffsetTarget>,
    ) -> Result<Self, ListShareGroupOffsetsPlanError> {
        Self::new(group_id, ListShareGroupOffsetsSelection::Selected(targets))
    }

    /// Returns the exact share-group coordinator key.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the exact validated all-or-selected partition intent.
    pub const fn selection(&self) -> &ListShareGroupOffsetsSelection {
        &self.selection
    }
}

fn validate_group(group_id: &str) -> Result<(), ListShareGroupOffsetsPlanError> {
    if group_id.is_empty() {
        return Err(ListShareGroupOffsetsPlanError::EmptyGroupId);
    }
    if group_id.len() > LIST_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES {
        return Err(ListShareGroupOffsetsPlanError::GroupIdTooLong);
    }
    Ok(())
}

fn validate_targets(
    group_bytes: usize,
    targets: &[ListShareGroupOffsetTarget],
) -> Result<(), ListShareGroupOffsetsPlanError> {
    if targets.is_empty() {
        return Err(ListShareGroupOffsetsPlanError::EmptySelection);
    }
    if targets.len() > LIST_SHARE_GROUP_OFFSETS_MAX_SELECTED_PARTITIONS {
        return Err(ListShareGroupOffsetsPlanError::TooManySelectedPartitions);
    }
    let mut text_bytes = group_bytes;
    let mut identities = BTreeSet::new();
    for target in targets {
        if target.topic.is_empty() {
            return Err(ListShareGroupOffsetsPlanError::EmptyTopicName);
        }
        if target.topic.len() > LIST_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES {
            return Err(ListShareGroupOffsetsPlanError::TopicNameTooLong);
        }
        if target.partition < 0 {
            return Err(ListShareGroupOffsetsPlanError::NegativePartition);
        }
        if !identities.insert((target.topic.as_str(), target.partition)) {
            return Err(ListShareGroupOffsetsPlanError::DuplicateTopicPartition);
        }
        text_bytes = text_bytes
            .checked_add(target.topic.len())
            .ok_or(ListShareGroupOffsetsPlanError::RequestTextTooLarge)?;
        if text_bytes > LIST_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES {
            return Err(ListShareGroupOffsetsPlanError::RequestTextTooLarge);
        }
    }
    Ok(())
}
