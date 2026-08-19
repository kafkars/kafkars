//! Engine-owned scalar intent for singular and batched `ShareGroup` offset listing.

use kafka_client_core::{
    ListShareGroupOffsetTarget as CoreTarget, ListShareGroupOffsetsPlan,
    ListShareGroupOffsetsPlanError, ListShareGroupOffsetsQuery,
};

/// One inert caller-ordered topic-partition selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsTarget {
    topic: String,
    partition: i32,
}

impl ListShareGroupOffsetsTarget {
    /// Creates one target for validation at the admission boundary.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }
}

/// Explicit inert request mode; selected and all partitions cannot be confused.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ListShareGroupOffsetsRequestSelection {
    Selected(Vec<ListShareGroupOffsetsTarget>),
    All,
}

/// One inert `ShareGroup` offset query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsRequest {
    group_id: String,
    selection: ListShareGroupOffsetsRequestSelection,
}

impl ListShareGroupOffsetsRequest {
    /// Creates one explicit all-partition query.
    pub const fn all(group_id: String) -> Self {
        Self {
            group_id,
            selection: ListShareGroupOffsetsRequestSelection::All,
        }
    }

    /// Creates one inert explicit topic-partition selection.
    pub const fn selected(group_id: String, targets: Vec<ListShareGroupOffsetsTarget>) -> Self {
        Self {
            group_id,
            selection: ListShareGroupOffsetsRequestSelection::Selected(targets),
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_id = canonical_string(self.group_id);
        if let ListShareGroupOffsetsRequestSelection::Selected(targets) = &mut self.selection {
            *targets = canonical_vec(
                core::mem::take(targets)
                    .into_iter()
                    .map(|target| ListShareGroupOffsetsTarget {
                        topic: canonical_string(target.topic),
                        partition: target.partition,
                    })
                    .collect(),
            );
        }
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<ListShareGroupOffsetsPlan, ListShareGroupOffsetsPlanError> {
        self.into_query().map(ListShareGroupOffsetsPlan::from_query)
    }

    fn into_query(self) -> Result<ListShareGroupOffsetsQuery, ListShareGroupOffsetsPlanError> {
        match self.selection {
            ListShareGroupOffsetsRequestSelection::Selected(targets) => {
                ListShareGroupOffsetsQuery::selected(
                    self.group_id,
                    targets
                        .into_iter()
                        .map(|target| CoreTarget::new(target.topic, target.partition))
                        .collect(),
                )
            }
            ListShareGroupOffsetsRequestSelection::All => {
                ListShareGroupOffsetsQuery::all(self.group_id)
            }
        }
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.group_id.capacity() == self.group_id.len()
            && match &self.selection {
                ListShareGroupOffsetsRequestSelection::All => true,
                ListShareGroupOffsetsRequestSelection::Selected(targets) => {
                    targets.capacity() == targets.len()
                        && targets
                            .iter()
                            .all(|target| target.topic.capacity() == target.topic.len())
                }
            }
    }
}

/// Caller-ordered share-group queries for one accepted batch operation.
///
/// Each entry preserves its own explicit all-or-selected partition mode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupsOffsetsRequest {
    queries: Vec<ListShareGroupOffsetsRequest>,
}

impl ListShareGroupsOffsetsRequest {
    /// Creates one inert batch for validation at the admission boundary.
    pub const fn new(queries: Vec<ListShareGroupOffsetsRequest>) -> Self {
        Self { queries }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.queries = canonical_vec(
            core::mem::take(&mut self.queries)
                .into_iter()
                .map(ListShareGroupOffsetsRequest::canonicalize)
                .collect(),
        );
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<ListShareGroupOffsetsPlan, ListShareGroupOffsetsPlanError> {
        let queries = self
            .queries
            .into_iter()
            .map(ListShareGroupOffsetsRequest::into_query)
            .collect::<Result<Vec<_>, _>>()?;
        ListShareGroupOffsetsPlan::batch(queries)
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.queries.capacity() == self.queries.len()
            && self
                .queries
                .iter()
                .all(ListShareGroupOffsetsRequest::storage_is_canonical)
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
