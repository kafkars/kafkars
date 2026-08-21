//! Linear public `ShareGroup` offset-listing intent translated at submission.

use crate::{TopicPartition, admin::ListShareGroupOffsetsQuery};

use super::engine::{
    GroupsRequest as EngineGroupsRequest, Request as EngineRequest, Target as EngineTarget,
};

// Kafka partitions are nonnegative. Preparing this sentinel before `submit`
// preserves assignment-only misuse until engine validation, after the public
// absolute deadline has been captured.
const INVALID_ASSIGNMENT_POSITION_PARTITION: i32 = i32::MIN;

enum Selection {
    All,
    Selected(Vec<TopicPartition>),
}

/// Request retained by the inert public builder before submission.
pub(crate) struct ListShareGroupOffsetsAdminRequest {
    group_id: String,
    selection: Selection,
}

impl ListShareGroupOffsetsAdminRequest {
    pub(crate) const fn all(group_id: String) -> Self {
        Self {
            group_id,
            selection: Selection::All,
        }
    }

    pub(crate) fn with_partitions(mut self, partitions: Vec<TopicPartition>) -> Self {
        self.selection = Selection::Selected(partitions);
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        match self.selection {
            Selection::All => EngineRequest::all(self.group_id),
            Selection::Selected(partitions) => EngineRequest::selected(
                self.group_id,
                partitions.into_iter().map(into_engine_target).collect(),
            ),
        }
    }
}

impl std::fmt::Debug for ListShareGroupOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListShareGroupOffsetsAdminRequest")
            .field("group_id", &self.group_id)
            .field(
                "selection",
                &match &self.selection {
                    Selection::All => "All",
                    Selection::Selected(_) => "Selected",
                },
            )
            .finish_non_exhaustive()
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

/// Multiple independently selected `ShareGroup` queries retained before admission.
pub(crate) struct ListShareGroupsOffsetsAdminRequest {
    queries: Vec<ListShareGroupOffsetsQuery>,
}

impl ListShareGroupsOffsetsAdminRequest {
    pub(crate) const fn new(queries: Vec<ListShareGroupOffsetsQuery>) -> Self {
        Self { queries }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineGroupsRequest {
        EngineGroupsRequest::new(self.queries.into_iter().map(into_engine_query).collect())
    }
}

impl std::fmt::Debug for ListShareGroupsOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListShareGroupsOffsetsAdminRequest")
            .field("query_count", &self.queries.len())
            .finish_non_exhaustive()
    }
}

fn into_engine_query(query: ListShareGroupOffsetsQuery) -> EngineRequest {
    let (group_id, partitions) = query.into_parts();
    match partitions {
        None => EngineRequest::all(group_id),
        Some(partitions) => EngineRequest::selected(
            group_id,
            partitions.into_iter().map(into_engine_target).collect(),
        ),
    }
}
