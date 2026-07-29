//! Validated all-or-selected topic-partition policy for one consumer group.

use std::collections::BTreeSet;

use super::{
    ListConsumerGroupOffsetsPlanError, MAX_CONSUMER_GROUP_ID_BYTES,
    MAX_CONSUMER_GROUP_REQUEST_TEXT_BYTES, MAX_SELECTED_PARTITIONS,
};

/// Maximum UTF-8 byte length accepted for one selected topic name.
pub(super) const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;
/// One caller-ordered topic-partition selected for offset description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetTarget {
    topic: String,
    partition: i32,
}

impl ListConsumerGroupOffsetTarget {
    /// Creates one inert target for validation by its enclosing query.
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

/// Explicit offset query mode; an empty selection never means all partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListConsumerGroupOffsetsSelection {
    /// Every committed topic-partition visible for the consumer group.
    All,
    /// One nonempty caller-ordered set of unique topic-partitions.
    Selected(Vec<ListConsumerGroupOffsetTarget>),
}

/// One validated consumer group and its exact partition-selection policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetsQuery {
    group_id: String,
    selection: ListConsumerGroupOffsetsSelection,
}

impl ListConsumerGroupOffsetsQuery {
    /// Validates one explicit group and all-or-selected query mode.
    pub fn new(
        group_id: String,
        selection: ListConsumerGroupOffsetsSelection,
    ) -> Result<Self, ListConsumerGroupOffsetsPlanError> {
        validate_group_id(&group_id)?;
        if let ListConsumerGroupOffsetsSelection::Selected(targets) = &selection {
            validate_targets(group_id.len(), targets)?;
        }
        Ok(Self {
            group_id,
            selection,
        })
    }

    /// Validates one all-partition query.
    pub fn all(group_id: String) -> Result<Self, ListConsumerGroupOffsetsPlanError> {
        Self::new(group_id, ListConsumerGroupOffsetsSelection::All)
    }

    /// Validates one selected-partition query.
    pub fn selected(
        group_id: String,
        targets: Vec<ListConsumerGroupOffsetTarget>,
    ) -> Result<Self, ListConsumerGroupOffsetsPlanError> {
        Self::new(
            group_id,
            ListConsumerGroupOffsetsSelection::Selected(targets),
        )
    }

    /// Returns the exact group coordinator key.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the exact validated partition-selection policy.
    pub const fn selection(&self) -> &ListConsumerGroupOffsetsSelection {
        &self.selection
    }

    pub(super) fn into_parts(self) -> (String, ListConsumerGroupOffsetsSelection) {
        (self.group_id, self.selection)
    }
}

fn validate_group_id(group_id: &str) -> Result<(), ListConsumerGroupOffsetsPlanError> {
    if group_id.is_empty() {
        return Err(ListConsumerGroupOffsetsPlanError::EmptyGroupId);
    }
    if group_id.len() > MAX_CONSUMER_GROUP_ID_BYTES {
        return Err(ListConsumerGroupOffsetsPlanError::GroupIdTooLong);
    }
    Ok(())
}

fn validate_targets(
    group_bytes: usize,
    targets: &[ListConsumerGroupOffsetTarget],
) -> Result<(), ListConsumerGroupOffsetsPlanError> {
    if targets.is_empty() {
        return Err(ListConsumerGroupOffsetsPlanError::EmptySelection);
    }
    if targets.len() > MAX_SELECTED_PARTITIONS {
        return Err(ListConsumerGroupOffsetsPlanError::TooManySelectedPartitions);
    }
    let mut identities = BTreeSet::new();
    let mut text_bytes = group_bytes;
    for target in targets {
        if target.topic.is_empty() {
            return Err(ListConsumerGroupOffsetsPlanError::EmptyTopicName);
        }
        if target.topic.len() > MAX_TOPIC_NAME_BYTES {
            return Err(ListConsumerGroupOffsetsPlanError::TopicNameTooLong);
        }
        if target.partition < 0 {
            return Err(ListConsumerGroupOffsetsPlanError::NegativePartition);
        }
        if !identities.insert((target.topic.as_str(), target.partition)) {
            return Err(ListConsumerGroupOffsetsPlanError::DuplicateTopicPartition);
        }
        text_bytes = text_bytes
            .checked_add(target.topic.len())
            .ok_or(ListConsumerGroupOffsetsPlanError::RequestTextTooLarge)?;
        if text_bytes > MAX_CONSUMER_GROUP_REQUEST_TEXT_BYTES {
            return Err(ListConsumerGroupOffsetsPlanError::RequestTextTooLarge);
        }
    }
    Ok(())
}
