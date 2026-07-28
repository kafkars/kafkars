//! Engine-owned scalar intent for Admin `AlterReplicaLogDirs`.

use kafka_client_core::{
    AlterReplicaLogDirAssignment as CoreAssignment, AlterReplicaLogDirsPlan,
    AlterReplicaLogDirsPlanError,
};

/// One caller-ordered replica-to-directory assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirAssignment {
    topic: String,
    partition: i32,
    broker_id: i32,
    target_path: String,
}

impl AlterReplicaLogDirAssignment {
    /// Creates inert assignment intent for validation at admission.
    pub const fn new(topic: String, partition: i32, broker_id: i32, target_path: String) -> Self {
        Self {
            topic,
            partition,
            broker_id,
            target_path,
        }
    }

    /// Consumes this assignment into stable scalar parts.
    pub fn into_parts(self) -> (String, i32, i32, String) {
        (self.topic, self.partition, self.broker_id, self.target_path)
    }

    fn canonicalize(mut self) -> Self {
        self.topic = self.topic.into_boxed_str().into_string();
        self.target_path = self.target_path.into_boxed_str().into_string();
        self
    }

    fn into_core(self) -> CoreAssignment {
        CoreAssignment::new(self.broker_id, self.topic, self.partition, self.target_path)
    }
}

/// One nonempty caller-ordered assignment batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirsRequest {
    assignments: Vec<AlterReplicaLogDirAssignment>,
}

impl AlterReplicaLogDirsRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(assignments: Vec<AlterReplicaLogDirAssignment>) -> Self {
        Self { assignments }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.assignments = self
            .assignments
            .into_iter()
            .map(AlterReplicaLogDirAssignment::canonicalize)
            .collect();
        self.assignments.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(self) -> Result<AlterReplicaLogDirsPlan, AlterReplicaLogDirsPlanError> {
        AlterReplicaLogDirsPlan::new(
            self.assignments
                .into_iter()
                .map(AlterReplicaLogDirAssignment::into_core)
                .collect(),
        )
    }
}
