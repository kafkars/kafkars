//! Inert consumer-group offset intent translated only at the engine boundary.

use kafka_client_engine::{
    ListConsumerGroupOffsetTarget as EngineTarget, ListConsumerGroupOffsetsQuery as EngineQuery,
    ListConsumerGroupOffsetsRequest as EngineRequest,
    ListConsumerGroupsOffsetsRequest as EngineBatchRequest,
};

use crate::{TopicPartition, admin::ListConsumerGroupOffsetsQuery};

// Kafka partitions are nonnegative. Preparing this sentinel before `submit`
// preserves assignment-only misuse until engine validation, after the public
// absolute deadline has been captured.
const INVALID_ASSIGNMENT_POSITION_PARTITION: i32 = i32::MIN;

enum Selection {
    All,
    Selected(Vec<TopicPartition>),
}

/// Linear request retained by the public builder before submission.
pub(crate) struct ListConsumerGroupOffsetsAdminRequest {
    group_id: String,
    selection: Selection,
    require_stable: bool,
}

impl ListConsumerGroupOffsetsAdminRequest {
    pub(crate) const fn all(group_id: String) -> Self {
        Self {
            group_id,
            selection: Selection::All,
            require_stable: false,
        }
    }

    pub(crate) const fn with_require_stable(mut self, require_stable: bool) -> Self {
        self.require_stable = require_stable;
        self
    }

    pub(crate) fn with_partitions(mut self, partitions: Vec<TopicPartition>) -> Self {
        self.selection = Selection::Selected(partitions);
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::from_query(
            into_engine_query_parts(self.group_id, self.selection),
            self.require_stable,
        )
    }
}

impl std::fmt::Debug for ListConsumerGroupOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupOffsetsAdminRequest")
            .field("group_id", &self.group_id)
            .field(
                "selection",
                &match &self.selection {
                    Selection::All => "All",
                    Selection::Selected(_) => "Selected",
                },
            )
            .field("require_stable", &self.require_stable)
            .finish_non_exhaustive()
    }
}

/// Linear plural request retained by the public builder before submission.
pub(crate) struct ListConsumerGroupsOffsetsAdminRequest {
    queries: Vec<ListConsumerGroupOffsetsQuery>,
    require_stable: bool,
}

impl ListConsumerGroupsOffsetsAdminRequest {
    pub(crate) const fn new(queries: Vec<ListConsumerGroupOffsetsQuery>) -> Self {
        Self {
            queries,
            require_stable: false,
        }
    }

    pub(crate) const fn with_require_stable(mut self, require_stable: bool) -> Self {
        self.require_stable = require_stable;
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineBatchRequest {
        EngineBatchRequest::from_queries(
            self.queries.into_iter().map(into_engine_query).collect(),
            self.require_stable,
        )
    }
}

impl std::fmt::Debug for ListConsumerGroupsOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupsOffsetsAdminRequest")
            .field("query_count", &self.queries.len())
            .field("require_stable", &self.require_stable)
            .finish_non_exhaustive()
    }
}

fn into_engine_query(query: ListConsumerGroupOffsetsQuery) -> EngineQuery {
    let (group_id, partitions) = query.into_parts();
    match partitions {
        None => EngineQuery::all(group_id),
        Some(partitions) => EngineQuery::selected(
            group_id,
            partitions.into_iter().map(into_engine_target).collect(),
        ),
    }
}

fn into_engine_query_parts(group_id: String, selection: Selection) -> EngineQuery {
    match selection {
        Selection::All => EngineQuery::all(group_id),
        Selection::Selected(partitions) => EngineQuery::selected(
            group_id,
            partitions.into_iter().map(into_engine_target).collect(),
        ),
    }
}

fn into_engine_target(target: TopicPartition) -> EngineTarget {
    let (topic, partition, start) = target.into_parts();
    let partition = if start.is_some() {
        INVALID_ASSIGNMENT_POSITION_PARTITION
    } else {
        partition
    };
    EngineTarget::new(topic, partition)
}
