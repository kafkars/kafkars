//! Singular preservation and caller-ordered batch composition for API-90 plans.

use std::collections::BTreeSet;

use super::{
    LIST_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES,
    LIST_SHARE_GROUP_OFFSETS_MAX_SELECTED_PARTITIONS, ListShareGroupOffsetTarget,
    ListShareGroupOffsetsPlanError, ListShareGroupOffsetsQuery, ListShareGroupOffsetsSelection,
};

/// Maximum share-group queries retained by one accepted operation.
pub const LIST_SHARE_GROUP_OFFSETS_MAX_GROUPS: usize = 16 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
enum ListShareGroupOffsetsPlanSelection {
    Singular(ListShareGroupOffsetsQuery),
    Batch(Vec<ListShareGroupOffsetsQuery>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListShareGroupOffsetsPlanShape {
    Singular,
    Batch,
}

/// Validated intent for one accepted singular or caller-ordered batch operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsPlan {
    queries: ListShareGroupOffsetsPlanSelection,
}

impl ListShareGroupOffsetsPlan {
    /// Validates one explicit group and explicit all-or-selected query mode.
    pub fn new(
        group_id: String,
        selection: ListShareGroupOffsetsSelection,
    ) -> Result<Self, ListShareGroupOffsetsPlanError> {
        let query = ListShareGroupOffsetsQuery::new(group_id, selection)?;
        Ok(Self::from_query(query))
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

    /// Wraps one already-validated query as the existing singular operation.
    pub const fn from_query(query: ListShareGroupOffsetsQuery) -> Self {
        Self {
            queries: ListShareGroupOffsetsPlanSelection::Singular(query),
        }
    }

    /// Validates one nonempty, unique, caller-ordered batch of group queries.
    pub fn batch(
        queries: Vec<ListShareGroupOffsetsQuery>,
    ) -> Result<Self, ListShareGroupOffsetsPlanError> {
        validate_batch(&queries)?;
        Ok(Self {
            queries: ListShareGroupOffsetsPlanSelection::Batch(queries),
        })
    }

    /// Returns the first exact share-group coordinator key.
    ///
    /// Every emitted `Submit` carries a one-query projection, preserving this
    /// accessor for the existing singleton protocol seam.
    pub fn group_id(&self) -> &str {
        self.queries()[0].group_id()
    }

    /// Returns the first query's exact all-or-selected partition intent.
    pub fn selection(&self) -> &ListShareGroupOffsetsSelection {
        match &self.queries {
            ListShareGroupOffsetsPlanSelection::Singular(query) => query.selection(),
            ListShareGroupOffsetsPlanSelection::Batch(queries) => queries[0].selection(),
        }
    }

    /// Returns every group query in exact caller order.
    pub fn queries(&self) -> &[ListShareGroupOffsetsQuery] {
        match &self.queries {
            ListShareGroupOffsetsPlanSelection::Singular(query) => core::slice::from_ref(query),
            ListShareGroupOffsetsPlanSelection::Batch(queries) => queries,
        }
    }

    pub(crate) fn shape(&self) -> ListShareGroupOffsetsPlanShape {
        match &self.queries {
            ListShareGroupOffsetsPlanSelection::Singular(_) => {
                ListShareGroupOffsetsPlanShape::Singular
            }
            ListShareGroupOffsetsPlanSelection::Batch(_) => ListShareGroupOffsetsPlanShape::Batch,
        }
    }

    pub(crate) fn singleton_at(&self, index: usize) -> Option<Self> {
        self.queries().get(index).cloned().map(Self::from_query)
    }
}

fn validate_batch(
    queries: &[ListShareGroupOffsetsQuery],
) -> Result<(), ListShareGroupOffsetsPlanError> {
    if queries.is_empty() {
        return Err(ListShareGroupOffsetsPlanError::EmptyGroupBatch);
    }
    if queries.len() > LIST_SHARE_GROUP_OFFSETS_MAX_GROUPS {
        return Err(ListShareGroupOffsetsPlanError::TooManyGroups);
    }
    let mut group_ids = BTreeSet::new();
    let mut request_text_bytes = 0usize;
    let mut selected_partitions = 0usize;
    for query in queries {
        if !group_ids.insert(query.group_id()) {
            return Err(ListShareGroupOffsetsPlanError::DuplicateGroupId);
        }
        request_text_bytes = request_text_bytes
            .checked_add(query.group_id().len())
            .ok_or(ListShareGroupOffsetsPlanError::RequestTextTooLarge)?;
        if let ListShareGroupOffsetsSelection::Selected(targets) = query.selection() {
            selected_partitions = selected_partitions
                .checked_add(targets.len())
                .ok_or(ListShareGroupOffsetsPlanError::TooManySelectedPartitions)?;
            for target in targets {
                request_text_bytes = request_text_bytes
                    .checked_add(target.topic().len())
                    .ok_or(ListShareGroupOffsetsPlanError::RequestTextTooLarge)?;
            }
        }
        if selected_partitions > LIST_SHARE_GROUP_OFFSETS_MAX_SELECTED_PARTITIONS {
            return Err(ListShareGroupOffsetsPlanError::TooManySelectedPartitions);
        }
        if request_text_bytes > LIST_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES {
            return Err(ListShareGroupOffsetsPlanError::RequestTextTooLarge);
        }
    }
    Ok(())
}
