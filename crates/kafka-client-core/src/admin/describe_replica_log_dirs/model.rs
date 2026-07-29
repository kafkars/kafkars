//! Validated caller-ordered replica selection and broker grouping.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum UTF-8 topic bytes accepted by the name-based API-35 request.
pub const DESCRIBE_REPLICA_LOG_DIRS_MAX_TOPIC_BYTES: usize = 249;

/// One topic-partition replica hosted by an exact broker.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DescribeReplicaLogDirsReplica {
    topic: String,
    partition: i32,
    broker_id: i32,
}

impl DescribeReplicaLogDirsReplica {
    /// Creates inert replica identity validated with its enclosing plan.
    pub const fn new(topic: String, partition: i32, broker_id: i32) -> Self {
        Self {
            topic,
            partition,
            broker_id,
        }
    }

    /// Returns the exact topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the signed partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the signed broker identity.
    pub const fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// Consumes the identity into adapter-owned scalar parts.
    pub fn into_parts(self) -> (String, i32, i32) {
        (self.topic, self.partition, self.broker_id)
    }
}

/// Validated caller order plus first-occurrence broker schedule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsPlan {
    replicas: Vec<DescribeReplicaLogDirsReplica>,
    broker_ids: Vec<i32>,
}

impl DescribeReplicaLogDirsPlan {
    /// Validates nonempty unique replicas and records first broker occurrence.
    pub fn new(
        replicas: Vec<DescribeReplicaLogDirsReplica>,
    ) -> Result<Self, DescribeReplicaLogDirsPlanError> {
        if replicas.is_empty() {
            return Err(DescribeReplicaLogDirsPlanError::EmptyReplicaBatch);
        }
        let mut identities = BTreeSet::new();
        let mut seen_brokers = BTreeSet::new();
        let mut broker_ids = Vec::new();
        for replica in &replicas {
            if replica.topic.is_empty() {
                return Err(DescribeReplicaLogDirsPlanError::EmptyTopic);
            }
            if replica.topic.len() > DESCRIBE_REPLICA_LOG_DIRS_MAX_TOPIC_BYTES {
                return Err(DescribeReplicaLogDirsPlanError::TopicTooLong);
            }
            if replica.partition < 0 {
                return Err(DescribeReplicaLogDirsPlanError::NegativePartition);
            }
            if replica.broker_id < 0 {
                return Err(DescribeReplicaLogDirsPlanError::NegativeBrokerId);
            }
            if !identities.insert((replica.broker_id, replica.topic.as_str(), replica.partition)) {
                return Err(DescribeReplicaLogDirsPlanError::DuplicateReplica);
            }
            if seen_brokers.insert(replica.broker_id) {
                broker_ids.push(replica.broker_id);
            }
        }
        Ok(Self {
            replicas,
            broker_ids,
        })
    }

    /// Returns replicas in exact caller order.
    pub fn replicas(&self) -> &[DescribeReplicaLogDirsReplica] {
        &self.replicas
    }

    /// Returns brokers in first caller-occurrence order.
    pub fn broker_ids(&self) -> &[i32] {
        &self.broker_ids
    }

    /// Iterates one broker's replicas in their relative caller order.
    pub fn replicas_for_broker(
        &self,
        broker_id: i32,
    ) -> impl Iterator<Item = &DescribeReplicaLogDirsReplica> {
        self.replicas
            .iter()
            .filter(move |replica| replica.broker_id == broker_id)
    }
}

/// Invalid deterministic replica log-directory request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsPlanError {
    /// At least one replica must be requested.
    EmptyReplicaBatch,
    /// Topic names cannot be empty.
    EmptyTopic,
    /// A topic exceeded the name-based request bound.
    TopicTooLong,
    /// Partition indexes must be nonnegative.
    NegativePartition,
    /// Broker IDs must be nonnegative.
    NegativeBrokerId,
    /// One operation cannot repeat an exact replica identity.
    DuplicateReplica,
}

impl fmt::Display for DescribeReplicaLogDirsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeReplicaLogDirs plan: {self:?}")
    }
}

impl std::error::Error for DescribeReplicaLogDirsPlanError {}
