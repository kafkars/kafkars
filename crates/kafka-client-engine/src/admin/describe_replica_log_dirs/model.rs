//! Engine-owned scalar intent for one Admin `DescribeReplicaLogDirs` query.

use kafka_client_core::{
    DescribeReplicaLogDirsPlan, DescribeReplicaLogDirsPlanError, DescribeReplicaLogDirsReplica,
};

/// One exact topic-partition replica target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsTarget {
    topic: String,
    partition: i32,
    broker_id: i32,
}

impl DescribeReplicaLogDirsTarget {
    /// Creates inert scalar intent validated at the operation boundary.
    pub const fn new(topic: String, partition: i32, broker_id: i32) -> Self {
        Self {
            topic,
            partition,
            broker_id,
        }
    }

    /// Returns the selected topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the selected partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the selected exact broker.
    pub const fn broker_id(&self) -> i32 {
        self.broker_id
    }

    pub(crate) fn into_core(self) -> DescribeReplicaLogDirsReplica {
        DescribeReplicaLogDirsReplica::new(self.topic, self.partition, self.broker_id)
    }
}

/// Caller-ordered selected replicas for one query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsRequest {
    targets: Vec<DescribeReplicaLogDirsTarget>,
}

impl DescribeReplicaLogDirsRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(targets: Vec<DescribeReplicaLogDirsTarget>) -> Self {
        Self { targets }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.targets.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<DescribeReplicaLogDirsPlan, DescribeReplicaLogDirsPlanFailure> {
        let mut replicas = Vec::new();
        replicas
            .try_reserve_exact(self.targets.len())
            .map_err(|_| DescribeReplicaLogDirsPlanFailure::RetainedBytes)?;
        replicas.extend(
            self.targets
                .into_iter()
                .map(DescribeReplicaLogDirsTarget::into_core),
        );
        DescribeReplicaLogDirsPlan::new(replicas)
            .map_err(DescribeReplicaLogDirsPlanFailure::Invalid)
    }
}

/// Request conversion failure before atomic host admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeReplicaLogDirsPlanFailure {
    /// Core rejected the selected identities.
    Invalid(DescribeReplicaLogDirsPlanError),
    /// Canonical core ownership could not fit an allocation.
    RetainedBytes,
}
