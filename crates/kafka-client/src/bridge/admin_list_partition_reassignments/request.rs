//! Inert reassignment selection translated only at the engine boundary.

use kafka_client_engine::{
    ListPartitionReassignmentTarget as EngineTarget,
    ListPartitionReassignmentsRequest as EngineRequest,
};

use crate::TopicPartition;

const INVALID_ASSIGNMENT_POSITION_PARTITION: i32 = i32::MIN;

/// Linear request retained by the public builder before submission.
pub(crate) struct ListPartitionReassignmentsAdminRequest {
    inner: EngineRequest,
}

impl ListPartitionReassignmentsAdminRequest {
    pub(crate) fn selected(targets: Vec<TopicPartition>) -> Self {
        Self {
            inner: EngineRequest::selected(targets.into_iter().map(into_engine_target).collect()),
        }
    }

    pub(crate) const fn all_active() -> Self {
        Self {
            inner: EngineRequest::all_active(),
        }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        self.inner
    }
}

impl std::fmt::Debug for ListPartitionReassignmentsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListPartitionReassignmentsAdminRequest")
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
