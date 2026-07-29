//! Inert selected replicas translated only at the engine boundary.

use crate::admin::TopicPartitionReplica;

use super::engine::{Request as EngineRequest, request, target};

/// Linear caller-ordered replicas retained by the public builder.
pub(crate) struct DescribeReplicaLogDirsAdminRequest {
    replicas: Vec<TopicPartitionReplica>,
}

impl DescribeReplicaLogDirsAdminRequest {
    pub(crate) const fn new(replicas: Vec<TopicPartitionReplica>) -> Self {
        Self { replicas }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        request(
            self.replicas
                .into_iter()
                .map(|replica| {
                    let (topic, partition, broker_id) = replica.into_parts();
                    target(topic, partition, broker_id)
                })
                .collect(),
        )
    }
}

impl std::fmt::Debug for DescribeReplicaLogDirsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeReplicaLogDirsAdminRequest")
            .field("replicas", &self.replicas)
            .finish()
    }
}
