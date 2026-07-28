//! Inert public reassignment intent translated only at the engine boundary.

use kafka_client_engine::{
    AlterPartitionReassignmentsRequest as EngineRequest,
    PartitionReassignmentChange as EngineChange,
};

use crate::PartitionReassignmentChange;

/// Linear request retained by the public builder before submission.
pub(crate) struct AlterPartitionReassignmentsAdminRequest {
    inner: EngineRequest,
}

impl AlterPartitionReassignmentsAdminRequest {
    pub(crate) fn new(changes: Vec<PartitionReassignmentChange>) -> Self {
        Self {
            inner: EngineRequest::new(changes.into_iter().map(into_engine_change).collect()),
        }
    }

    pub(crate) fn with_allow_replication_factor_change(mut self, allow: bool) -> Self {
        self.inner = self.inner.with_allow_replication_factor_change(allow);
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        self.inner
    }
}

impl std::fmt::Debug for AlterPartitionReassignmentsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterPartitionReassignmentsAdminRequest")
            .finish_non_exhaustive()
    }
}

fn into_engine_change(change: PartitionReassignmentChange) -> EngineChange {
    let (topic, partition, replicas) = change.into_parts();
    match replicas {
        Some(replicas) => EngineChange::replace(topic, partition, replicas),
        None => EngineChange::cancel(topic, partition),
    }
}
