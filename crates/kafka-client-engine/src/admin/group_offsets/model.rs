//! Engine-owned scalar intent for singular and batched consumer-group queries.

use kafka_client_core::{
    ListConsumerGroupOffsetTarget as CoreTarget, ListConsumerGroupOffsetsPlan,
    ListConsumerGroupOffsetsPlanError, ListConsumerGroupOffsetsQuery as CoreQuery,
    ListConsumerGroupOffsetsSelection as CoreSelection,
};

/// One selected topic-partition whose committed offset should be returned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupOffsetTarget {
    topic: String,
    partition: i32,
}

impl ListConsumerGroupOffsetTarget {
    /// Creates one inert target for validation at the submission boundary.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    fn canonicalize(mut self) -> Self {
        self.topic = self.topic.into_boxed_str().into_string();
        self
    }

    fn into_core(self) -> CoreTarget {
        CoreTarget::new(self.topic, self.partition)
    }

    #[cfg(test)]
    fn storage_is_canonical(&self) -> bool {
        self.topic.capacity() == self.topic.len()
    }
}

/// Partition selection retained by one inert consumer-group offset query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupOffsetsSelection {
    /// Requests every committed offset visible for the group.
    All,
    /// Requests exactly the caller-ordered topic-partitions.
    Selected(Vec<ListConsumerGroupOffsetTarget>),
}

impl ListConsumerGroupOffsetsSelection {
    fn canonicalize(self) -> Self {
        match self {
            Self::All => Self::All,
            Self::Selected(targets) => Self::Selected(
                targets
                    .into_iter()
                    .map(ListConsumerGroupOffsetTarget::canonicalize)
                    .collect::<Vec<_>>()
                    .into_boxed_slice()
                    .into_vec(),
            ),
        }
    }

    fn into_core(self) -> CoreSelection {
        match self {
            Self::All => CoreSelection::All,
            Self::Selected(targets) => CoreSelection::Selected(
                targets
                    .into_iter()
                    .map(ListConsumerGroupOffsetTarget::into_core)
                    .collect(),
            ),
        }
    }

    #[cfg(test)]
    fn storage_is_canonical(&self) -> bool {
        match self {
            Self::All => true,
            Self::Selected(targets) => {
                targets.capacity() == targets.len()
                    && targets
                        .iter()
                        .all(ListConsumerGroupOffsetTarget::storage_is_canonical)
            }
        }
    }
}

/// One explicit group identity and its per-group offset selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupOffsetsQuery {
    group_id: String,
    selection: ListConsumerGroupOffsetsSelection,
}

impl ListConsumerGroupOffsetsQuery {
    /// Creates an all-partition query for one group.
    pub const fn all(group_id: String) -> Self {
        Self {
            group_id,
            selection: ListConsumerGroupOffsetsSelection::All,
        }
    }

    /// Creates an exact caller-ordered partition query for one group.
    pub const fn selected(group_id: String, targets: Vec<ListConsumerGroupOffsetTarget>) -> Self {
        Self {
            group_id,
            selection: ListConsumerGroupOffsetsSelection::Selected(targets),
        }
    }

    fn canonicalize(mut self) -> Self {
        self.group_id = self.group_id.into_boxed_str().into_string();
        self.selection = self.selection.canonicalize();
        self
    }

    fn into_core(self) -> Result<CoreQuery, ListConsumerGroupOffsetsPlanError> {
        CoreQuery::new(self.group_id, self.selection.into_core())
    }

    #[cfg(test)]
    fn storage_is_canonical(&self) -> bool {
        self.group_id.capacity() == self.group_id.len() && self.selection.storage_is_canonical()
    }
}

/// One query for an explicit consumer group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupOffsetsRequest {
    query: ListConsumerGroupOffsetsQuery,
    require_stable: bool,
}

impl ListConsumerGroupOffsetsRequest {
    /// Creates one inert all-partition request for validation at admission.
    pub const fn new(group_id: String, require_stable: bool) -> Self {
        Self {
            query: ListConsumerGroupOffsetsQuery::all(group_id),
            require_stable,
        }
    }

    /// Creates one inert request with an explicit per-group selection.
    pub const fn from_query(query: ListConsumerGroupOffsetsQuery, require_stable: bool) -> Self {
        Self {
            query,
            require_stable,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.query = self.query.canonicalize();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsPlanError> {
        Ok(ListConsumerGroupOffsetsPlan::from_query(
            self.query.into_core()?,
            self.require_stable,
        ))
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.query.storage_is_canonical()
    }
}

/// Caller-ordered per-group offset queries for multiple consumer groups.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupsOffsetsRequest {
    queries: Vec<ListConsumerGroupOffsetsQuery>,
    require_stable: bool,
}

impl ListConsumerGroupsOffsetsRequest {
    /// Creates one inert all-partition batch for validation at admission.
    pub fn new(group_ids: Vec<String>, require_stable: bool) -> Self {
        Self {
            queries: group_ids
                .into_iter()
                .map(ListConsumerGroupOffsetsQuery::all)
                .collect(),
            require_stable,
        }
    }

    /// Creates one inert batch retaining each group's exact selection.
    pub const fn from_queries(
        queries: Vec<ListConsumerGroupOffsetsQuery>,
        require_stable: bool,
    ) -> Self {
        Self {
            queries,
            require_stable,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.queries = self
            .queries
            .into_iter()
            .map(ListConsumerGroupOffsetsQuery::canonicalize)
            .collect::<Vec<_>>()
            .into_boxed_slice()
            .into_vec();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsPlanError> {
        ListConsumerGroupOffsetsPlan::new_query_batch(
            self.queries
                .into_iter()
                .map(ListConsumerGroupOffsetsQuery::into_core)
                .collect::<Result<Vec<_>, _>>()?,
            self.require_stable,
        )
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.queries.capacity() == self.queries.len()
            && self
                .queries
                .iter()
                .all(ListConsumerGroupOffsetsQuery::storage_is_canonical)
    }
}
