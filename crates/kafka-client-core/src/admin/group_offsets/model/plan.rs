//! Singular and batched consumer-group offset plan ownership and validation.

use std::collections::BTreeSet;

use super::{
    ListConsumerGroupOffsetsPlanError, ListConsumerGroupOffsetsQuery,
    ListConsumerGroupOffsetsSelection, MAX_CONSUMER_GROUP_ID_BYTES,
    MAX_CONSUMER_GROUP_REQUEST_TEXT_BYTES, MAX_CONSUMER_GROUPS, MAX_SELECTED_PARTITIONS,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum ListConsumerGroupOffsetsPlanSelection {
    Singular {
        group_id: String,
        partition_selection: ListConsumerGroupOffsetsSelection,
    },
    Batch {
        group_ids: Vec<String>,
        partition_selections: Vec<ListConsumerGroupOffsetsSelection>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListConsumerGroupOffsetsPlanShape {
    Singular,
    Batch,
}

/// Validated intent for one accepted consumer-group offset operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetsPlan {
    selection: ListConsumerGroupOffsetsPlanSelection,
    require_stable: bool,
}

impl ListConsumerGroupOffsetsPlan {
    /// Validates one explicit all-partition group query.
    pub fn new(
        group_id: String,
        require_stable: bool,
    ) -> Result<Self, ListConsumerGroupOffsetsPlanError> {
        validate_group_ids(core::slice::from_ref(&group_id))?;
        Ok(Self {
            selection: ListConsumerGroupOffsetsPlanSelection::Singular {
                group_id,
                partition_selection: ListConsumerGroupOffsetsSelection::All,
            },
            require_stable,
        })
    }

    /// Validates a nonempty caller-ordered all-partition group batch.
    pub fn new_batch(
        group_ids: Vec<String>,
        require_stable: bool,
    ) -> Result<Self, ListConsumerGroupOffsetsPlanError> {
        if group_ids.is_empty() {
            return Err(ListConsumerGroupOffsetsPlanError::EmptyGroupBatch);
        }
        validate_group_ids(&group_ids)?;
        let partition_selections = vec![ListConsumerGroupOffsetsSelection::All; group_ids.len()];
        Ok(Self {
            selection: ListConsumerGroupOffsetsPlanSelection::Batch {
                group_ids,
                partition_selections,
            },
            require_stable,
        })
    }

    /// Wraps one already-validated query as a singular operation.
    pub fn from_query(query: ListConsumerGroupOffsetsQuery, require_stable: bool) -> Self {
        let (group_id, partition_selection) = query.into_parts();
        Self {
            selection: ListConsumerGroupOffsetsPlanSelection::Singular {
                group_id,
                partition_selection,
            },
            require_stable,
        }
    }

    /// Validates a nonempty caller-ordered batch of distinct group queries.
    pub fn new_query_batch(
        queries: Vec<ListConsumerGroupOffsetsQuery>,
        require_stable: bool,
    ) -> Result<Self, ListConsumerGroupOffsetsPlanError> {
        validate_queries(&queries)?;
        let mut group_ids = Vec::with_capacity(queries.len());
        let mut partition_selections = Vec::with_capacity(queries.len());
        for query in queries {
            let (group_id, selection) = query.into_parts();
            group_ids.push(group_id);
            partition_selections.push(selection);
        }
        Ok(Self {
            selection: ListConsumerGroupOffsetsPlanSelection::Batch {
                group_ids,
                partition_selections,
            },
            require_stable,
        })
    }

    /// Returns the first exact UTF-8 group identity.
    ///
    /// Every emitted `Submit` effect carries a one-group projection.
    pub fn group_id(&self) -> &str {
        match &self.selection {
            ListConsumerGroupOffsetsPlanSelection::Singular { group_id, .. } => group_id,
            ListConsumerGroupOffsetsPlanSelection::Batch { group_ids, .. } => &group_ids[0],
        }
    }

    /// Returns exact group identities in caller order.
    pub fn group_ids(&self) -> &[String] {
        match &self.selection {
            ListConsumerGroupOffsetsPlanSelection::Singular { group_id, .. } => {
                core::slice::from_ref(group_id)
            }
            ListConsumerGroupOffsetsPlanSelection::Batch { group_ids, .. } => group_ids,
        }
    }

    /// Returns the first group's exact all-or-selected partition policy.
    pub fn selection(&self) -> &ListConsumerGroupOffsetsSelection {
        &self.selections()[0]
    }

    /// Returns each group's partition policy in matching caller order.
    pub fn selections(&self) -> &[ListConsumerGroupOffsetsSelection] {
        match &self.selection {
            ListConsumerGroupOffsetsPlanSelection::Singular {
                partition_selection,
                ..
            } => core::slice::from_ref(partition_selection),
            ListConsumerGroupOffsetsPlanSelection::Batch {
                partition_selections,
                ..
            } => partition_selections,
        }
    }

    /// Returns whether Kafka must reject unstable group state.
    pub const fn require_stable(&self) -> bool {
        self.require_stable
    }

    pub(crate) fn shape(&self) -> ListConsumerGroupOffsetsPlanShape {
        match &self.selection {
            ListConsumerGroupOffsetsPlanSelection::Singular { .. } => {
                ListConsumerGroupOffsetsPlanShape::Singular
            }
            ListConsumerGroupOffsetsPlanSelection::Batch { .. } => {
                ListConsumerGroupOffsetsPlanShape::Batch
            }
        }
    }

    pub(crate) fn singleton_at(&self, index: usize) -> Option<Self> {
        self.group_ids()
            .get(index)
            .cloned()
            .zip(self.selections().get(index).cloned())
            .map(|(group_id, partition_selection)| Self {
                selection: ListConsumerGroupOffsetsPlanSelection::Singular {
                    group_id,
                    partition_selection,
                },
                require_stable: self.require_stable,
            })
    }
}

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

fn validate_queries(
    queries: &[ListConsumerGroupOffsetsQuery],
) -> Result<(), ListConsumerGroupOffsetsPlanError> {
    if queries.is_empty() {
        return Err(ListConsumerGroupOffsetsPlanError::EmptyGroupBatch);
    }
    if queries.len() > MAX_CONSUMER_GROUPS {
        return Err(ListConsumerGroupOffsetsPlanError::TooManyGroups);
    }
    let mut groups = BTreeSet::new();
    let mut selected_partitions = 0usize;
    let mut text_bytes = 0usize;
    for query in queries {
        if !groups.insert(query.group_id()) {
            return Err(ListConsumerGroupOffsetsPlanError::DuplicateGroupId);
        }
        text_bytes = text_bytes
            .checked_add(query.group_id().len())
            .ok_or(ListConsumerGroupOffsetsPlanError::RequestTextTooLarge)?;
        if let ListConsumerGroupOffsetsSelection::Selected(targets) = query.selection() {
            selected_partitions = selected_partitions
                .checked_add(targets.len())
                .ok_or(ListConsumerGroupOffsetsPlanError::TooManySelectedPartitions)?;
            for target in targets {
                text_bytes = text_bytes
                    .checked_add(target.topic().len())
                    .ok_or(ListConsumerGroupOffsetsPlanError::RequestTextTooLarge)?;
            }
        }
        if selected_partitions > MAX_SELECTED_PARTITIONS {
            return Err(ListConsumerGroupOffsetsPlanError::TooManySelectedPartitions);
        }
        if text_bytes > MAX_CONSUMER_GROUP_REQUEST_TEXT_BYTES {
            return Err(ListConsumerGroupOffsetsPlanError::RequestTextTooLarge);
        }
    }
    Ok(())
}
