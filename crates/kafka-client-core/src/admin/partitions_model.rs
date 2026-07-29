//! Validated input for one ordered `CreatePartitions` batch.

use core::fmt;
use std::collections::BTreeSet;

/// One topic and its requested new total partition count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsSpecification {
    topic: String,
    total_count: i32,
    replica_assignments: Option<Vec<Vec<i32>>>,
}

impl CreatePartitionsSpecification {
    /// Creates one automatic-assignment partition increase.
    pub const fn new(topic: String, total_count: i32) -> Self {
        Self {
            topic,
            total_count,
            replica_assignments: None,
        }
    }

    /// Creates one partition increase with exact assignments for new partitions.
    pub const fn with_replica_assignments(
        topic: String,
        total_count: i32,
        replica_assignments: Vec<Vec<i32>>,
    ) -> Self {
        Self {
            topic,
            total_count,
            replica_assignments: Some(replica_assignments),
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the requested new total partition count.
    pub const fn total_count(&self) -> i32 {
        self.total_count
    }

    /// Returns exact caller-ordered assignments or `None` for broker placement.
    pub fn replica_assignments(&self) -> Option<&[Vec<i32>]> {
        self.replica_assignments.as_deref()
    }
}

/// Ordered validated policy input for one `CreatePartitions` RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsPlan {
    topics: Vec<CreatePartitionsSpecification>,
    validate_only: bool,
}

impl CreatePartitionsPlan {
    /// Validates a nonempty batch with unique names and positive total counts.
    pub fn new(
        topics: Vec<CreatePartitionsSpecification>,
        validate_only: bool,
    ) -> Result<Self, CreatePartitionsPlanError> {
        if topics.is_empty() {
            return Err(CreatePartitionsPlanError::EmptyBatch);
        }
        let mut names = BTreeSet::new();
        for topic in &topics {
            if topic.topic.is_empty() {
                return Err(CreatePartitionsPlanError::EmptyTopicName);
            }
            if topic.total_count <= 0 {
                return Err(CreatePartitionsPlanError::InvalidTotalCount);
            }
            if !names.insert(topic.topic.as_str()) {
                return Err(CreatePartitionsPlanError::DuplicateTopic);
            }
            validate_replica_assignments(topic.replica_assignments.as_deref())?;
        }
        Ok(Self {
            topics,
            validate_only,
        })
    }

    /// Returns requests in original caller order.
    pub fn topics(&self) -> &[CreatePartitionsSpecification] {
        &self.topics
    }

    /// Returns whether Kafka should validate without mutating.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }
}

/// Invalid deterministic `CreatePartitions` input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreatePartitionsPlanError {
    /// Kafka cannot execute an empty batch.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// New total partition counts must be positive.
    InvalidTotalCount,
    /// Topic names in one batch must be unique.
    DuplicateTopic,
    /// An explicit assignment must name at least one newly added partition.
    EmptyReplicaAssignments,
    /// Every newly added partition must name at least one replica broker.
    EmptyReplicaAssignment,
    /// Kafka broker IDs must be nonnegative.
    InvalidReplicaBrokerId,
    /// One broker ID may occur only once within a partition assignment.
    DuplicateReplicaBrokerId,
}

impl fmt::Display for CreatePartitionsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBatch => "CreatePartitions batch is empty",
            Self::EmptyTopicName => "CreatePartitions topic name is empty",
            Self::InvalidTotalCount => "CreatePartitions total count must be positive",
            Self::DuplicateTopic => "CreatePartitions batch contains a duplicate topic",
            Self::EmptyReplicaAssignments => {
                "CreatePartitions explicit replica assignments are empty"
            }
            Self::EmptyReplicaAssignment => {
                "CreatePartitions contains an empty partition replica assignment"
            }
            Self::InvalidReplicaBrokerId => {
                "CreatePartitions replica assignments contain a negative broker ID"
            }
            Self::DuplicateReplicaBrokerId => {
                "CreatePartitions partition assignment contains a duplicate broker ID"
            }
        })
    }
}

impl std::error::Error for CreatePartitionsPlanError {}

fn validate_replica_assignments(
    assignments: Option<&[Vec<i32>]>,
) -> Result<(), CreatePartitionsPlanError> {
    let Some(assignments) = assignments else {
        return Ok(());
    };
    if assignments.is_empty() {
        return Err(CreatePartitionsPlanError::EmptyReplicaAssignments);
    }
    for assignment in assignments {
        if assignment.is_empty() {
            return Err(CreatePartitionsPlanError::EmptyReplicaAssignment);
        }
        let mut brokers = BTreeSet::new();
        for broker_id in assignment {
            if *broker_id < 0 {
                return Err(CreatePartitionsPlanError::InvalidReplicaBrokerId);
            }
            if !brokers.insert(*broker_id) {
                return Err(CreatePartitionsPlanError::DuplicateReplicaBrokerId);
            }
        }
    }
    Ok(())
}
