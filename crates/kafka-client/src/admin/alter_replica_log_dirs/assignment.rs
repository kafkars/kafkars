//! Explicit target-path assignment for one topic-partition replica.

use super::TopicPartitionReplica;

/// Inert request to move one replica log to a broker-local directory path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaLogDirAssignment {
    replica: TopicPartitionReplica,
    target_path: String,
}

impl ReplicaLogDirAssignment {
    /// Creates one explicit replica-to-directory assignment.
    pub fn new(replica: TopicPartitionReplica, target_path: impl Into<String>) -> Self {
        Self {
            replica,
            target_path: target_path.into(),
        }
    }

    /// Returns the selected replica identity.
    pub const fn replica(&self) -> &TopicPartitionReplica {
        &self.replica
    }

    /// Returns the requested broker-local directory path.
    pub fn target_path(&self) -> &str {
        &self.target_path
    }

    pub(crate) fn into_parts(self) -> (TopicPartitionReplica, String) {
        (self.replica, self.target_path)
    }
}
