//! Inert public assignments translated only at the engine boundary.

use crate::admin::ReplicaLogDirAssignment;

use super::engine::{Request as EngineRequest, assignment, request};

/// Linear caller-ordered assignments retained by the public builder.
pub(crate) struct AlterReplicaLogDirsAdminRequest {
    assignments: Vec<ReplicaLogDirAssignment>,
}

impl AlterReplicaLogDirsAdminRequest {
    pub(crate) const fn new(assignments: Vec<ReplicaLogDirAssignment>) -> Self {
        Self { assignments }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        request(
            self.assignments
                .into_iter()
                .map(|assignment_value| {
                    let (replica, target_path) = assignment_value.into_parts();
                    let (topic, partition, broker_id) = replica.into_parts();
                    assignment(topic, partition, broker_id, target_path)
                })
                .collect(),
        )
    }
}

impl std::fmt::Debug for AlterReplicaLogDirsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterReplicaLogDirsAdminRequest")
            .field("assignments", &self.assignments)
            .finish()
    }
}
