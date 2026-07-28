//! Validated caller-ordered replica-to-directory assignment intent.

use core::fmt;
use std::collections::BTreeSet;

const MAX_TOPIC_NAME_BYTES: usize = 249;
const MAX_LOG_DIR_BYTES: usize = i16::MAX as usize;

/// One replica and its requested target log-directory path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirAssignment {
    broker_id: i32,
    topic: String,
    partition: i32,
    log_dir: String,
}

impl AlterReplicaLogDirAssignment {
    /// Creates one assignment for validation by the enclosing request plan.
    pub const fn new(broker_id: i32, topic: String, partition: i32, log_dir: String) -> Self {
        Self {
            broker_id,
            topic,
            partition,
            log_dir,
        }
    }

    /// Returns the exact target broker.
    pub const fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// Returns the exact topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the requested nonempty log-directory path.
    pub fn log_dir(&self) -> &str {
        &self.log_dir
    }

    /// Consumes the assignment into adapter-owned parts.
    pub fn into_parts(self) -> (i32, String, i32, String) {
        (self.broker_id, self.topic, self.partition, self.log_dir)
    }
}

/// Validated intent for one bounded `AlterReplicaLogDirs` operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirsPlan {
    assignments: Vec<AlterReplicaLogDirAssignment>,
    broker_ids: Vec<i32>,
}

impl AlterReplicaLogDirsPlan {
    /// Validates unique assignments and retains first-appearance broker order.
    pub fn new(
        assignments: Vec<AlterReplicaLogDirAssignment>,
    ) -> Result<Self, AlterReplicaLogDirsPlanError> {
        if assignments.is_empty() {
            return Err(AlterReplicaLogDirsPlanError::EmptyAssignmentBatch);
        }
        let mut identities = BTreeSet::new();
        let mut seen_brokers = BTreeSet::new();
        let mut broker_ids = Vec::new();
        for assignment in &assignments {
            validate_assignment(assignment)?;
            if !identities.insert((
                assignment.broker_id,
                assignment.topic.as_str(),
                assignment.partition,
            )) {
                return Err(AlterReplicaLogDirsPlanError::DuplicateReplica);
            }
            if seen_brokers.insert(assignment.broker_id) {
                broker_ids.push(assignment.broker_id);
            }
        }
        drop(identities);
        Ok(Self {
            assignments,
            broker_ids,
        })
    }

    /// Returns assignments in exact caller order.
    pub fn assignments(&self) -> &[AlterReplicaLogDirAssignment] {
        &self.assignments
    }

    /// Returns unique brokers in deterministic first-appearance order.
    pub fn broker_ids(&self) -> &[i32] {
        &self.broker_ids
    }
}

fn validate_assignment(
    assignment: &AlterReplicaLogDirAssignment,
) -> Result<(), AlterReplicaLogDirsPlanError> {
    if assignment.broker_id < 0 {
        return Err(AlterReplicaLogDirsPlanError::NegativeBrokerId);
    }
    if assignment.topic.is_empty() {
        return Err(AlterReplicaLogDirsPlanError::EmptyTopicName);
    }
    if assignment.topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(AlterReplicaLogDirsPlanError::TopicNameTooLong);
    }
    if assignment.partition < 0 {
        return Err(AlterReplicaLogDirsPlanError::NegativePartition);
    }
    if assignment.log_dir.is_empty() {
        return Err(AlterReplicaLogDirsPlanError::EmptyLogDir);
    }
    if assignment.log_dir.len() > MAX_LOG_DIR_BYTES {
        return Err(AlterReplicaLogDirsPlanError::LogDirTooLong);
    }
    Ok(())
}

/// Invalid deterministic replica log-directory assignment intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsPlanError {
    /// At least one replica assignment must be requested.
    EmptyAssignmentBatch,
    /// Broker IDs must be nonnegative.
    NegativeBrokerId,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// Topic names must fit Kafka's supported topic-name domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// Target log-directory paths must not be empty.
    EmptyLogDir,
    /// Target paths must fit Kafka's string domain.
    LogDirTooLong,
    /// One operation cannot assign the same broker replica twice.
    DuplicateReplica,
}

impl fmt::Display for AlterReplicaLogDirsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyAssignmentBatch => "AlterReplicaLogDirs assignment batch is empty",
            Self::NegativeBrokerId => "AlterReplicaLogDirs broker ID is negative",
            Self::EmptyTopicName => "AlterReplicaLogDirs topic is empty",
            Self::TopicNameTooLong => "AlterReplicaLogDirs topic is too long",
            Self::NegativePartition => "AlterReplicaLogDirs partition is negative",
            Self::EmptyLogDir => "AlterReplicaLogDirs target path is empty",
            Self::LogDirTooLong => "AlterReplicaLogDirs target path is too long",
            Self::DuplicateReplica => "AlterReplicaLogDirs contains a duplicate broker replica",
        })
    }
}

impl std::error::Error for AlterReplicaLogDirsPlanError {}
