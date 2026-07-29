//! Validated semantic input for one batched `CreateTopics` operation.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum manually assigned partitions retained for one topic.
pub const CREATE_TOPICS_MAX_MANUAL_PARTITIONS_PER_TOPIC: usize = 32 * 1024;
/// Maximum replicas retained for one manually assigned partition.
pub const CREATE_TOPICS_MAX_REPLICAS_PER_PARTITION: usize = 4 * 1024;
/// Maximum manual broker references retained across one request.
pub const CREATE_TOPICS_MAX_MANUAL_BROKER_REFERENCES: usize = 256 * 1024;

/// One nullable topic configuration entry, retained in caller order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicConfig {
    name: String,
    value: Option<String>,
}

impl CreateTopicConfig {
    /// Creates one semantic topic configuration entry.
    pub fn new(name: impl Into<String>, value: Option<String>) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    /// Returns the configuration name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the optional configuration value.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }
}

/// One explicit partition-to-broker assignment retained in caller order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicReplicaAssignment {
    partition_index: i32,
    broker_ids: Vec<i32>,
}

impl CreateTopicReplicaAssignment {
    /// Creates one unvalidated manual placement entry.
    pub const fn new(partition_index: i32, broker_ids: Vec<i32>) -> Self {
        Self {
            partition_index,
            broker_ids,
        }
    }

    /// Returns the exact partition index.
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns broker IDs in replica order.
    pub fn broker_ids(&self) -> &[i32] {
        &self.broker_ids
    }
}

/// Explicit automatic or manual replica-placement intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTopicPlacement {
    /// Kafka chooses placement from count and replication-factor facts.
    Automatic {
        /// Positive partition count.
        partitions: i32,
        /// Positive replication factor, or Kafka's `-1` default sentinel.
        replication_factor: i16,
    },
    /// The request names every partition and its replicas.
    Manual {
        /// Caller-ordered manual assignments.
        assignments: Vec<CreateTopicReplicaAssignment>,
        /// A conflicting public replication-factor setting, when supplied.
        conflicting_replication_factor: Option<i16>,
    },
}

/// One topic specification in a batched `CreateTopics` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicSpecification {
    name: String,
    placement: CreateTopicPlacement,
    configs: Vec<CreateTopicConfig>,
}

impl CreateTopicSpecification {
    /// Creates one topic specification with automatic replica assignment.
    pub fn new(
        name: impl Into<String>,
        partitions: i32,
        replication_factor: i16,
        configs: Vec<CreateTopicConfig>,
    ) -> Self {
        Self {
            name: name.into(),
            placement: CreateTopicPlacement::Automatic {
                partitions,
                replication_factor,
            },
            configs,
        }
    }

    /// Creates one topic with explicit manual partition placement.
    pub fn manual(
        name: impl Into<String>,
        assignments: Vec<CreateTopicReplicaAssignment>,
        conflicting_replication_factor: Option<i16>,
        configs: Vec<CreateTopicConfig>,
    ) -> Self {
        Self {
            name: name.into(),
            placement: CreateTopicPlacement::Manual {
                assignments,
                conflicting_replication_factor,
            },
            configs,
        }
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the requested partition count.
    pub const fn partitions(&self) -> i32 {
        match &self.placement {
            CreateTopicPlacement::Automatic { partitions, .. } => *partitions,
            CreateTopicPlacement::Manual { .. } => -1,
        }
    }

    /// Returns the replication factor, or `-1` for the broker default.
    pub const fn replication_factor(&self) -> i16 {
        match &self.placement {
            CreateTopicPlacement::Automatic {
                replication_factor, ..
            } => *replication_factor,
            CreateTopicPlacement::Manual { .. } => -1,
        }
    }

    /// Returns the explicit placement intent.
    pub const fn placement(&self) -> &CreateTopicPlacement {
        &self.placement
    }

    /// Returns configuration entries in caller order.
    pub fn configs(&self) -> &[CreateTopicConfig] {
        &self.configs
    }
}

/// Ordered, validated policy input for one `CreateTopics` RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsPlan {
    topics: Vec<CreateTopicSpecification>,
    validate_only: bool,
}

impl CreateTopicsPlan {
    /// Validates a nonempty, uniquely named `CreateTopics` batch.
    pub fn new(
        topics: Vec<CreateTopicSpecification>,
        validate_only: bool,
    ) -> Result<Self, CreateTopicsPlanError> {
        if topics.is_empty() {
            return Err(CreateTopicsPlanError::EmptyBatch);
        }
        let mut names = BTreeSet::new();
        let mut broker_references = 0usize;
        for topic in &topics {
            validate_topic(topic, &mut broker_references)?;
            if !names.insert(topic.name()) {
                return Err(CreateTopicsPlanError::DuplicateTopic);
            }
        }
        Ok(Self {
            topics,
            validate_only,
        })
    }

    /// Returns topic specifications in caller order.
    pub fn topics(&self) -> &[CreateTopicSpecification] {
        &self.topics
    }

    /// Returns whether Kafka should validate without creating topics.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }
}

fn validate_topic(
    topic: &CreateTopicSpecification,
    broker_references: &mut usize,
) -> Result<(), CreateTopicsPlanError> {
    if topic.name.is_empty() {
        return Err(CreateTopicsPlanError::EmptyTopicName);
    }
    match &topic.placement {
        CreateTopicPlacement::Automatic {
            partitions,
            replication_factor,
        } => {
            if *partitions <= 0 {
                return Err(CreateTopicsPlanError::InvalidPartitionCount);
            }
            if *replication_factor != -1 && *replication_factor <= 0 {
                return Err(CreateTopicsPlanError::InvalidReplicationFactor);
            }
        }
        CreateTopicPlacement::Manual {
            assignments,
            conflicting_replication_factor,
        } => validate_manual(
            assignments,
            *conflicting_replication_factor,
            broker_references,
        )?,
    }
    if topic.configs.iter().any(|config| config.name.is_empty()) {
        return Err(CreateTopicsPlanError::EmptyConfigName);
    }
    Ok(())
}

fn validate_manual(
    assignments: &[CreateTopicReplicaAssignment],
    conflicting_replication_factor: Option<i16>,
    broker_references: &mut usize,
) -> Result<(), CreateTopicsPlanError> {
    if conflicting_replication_factor.is_some() {
        return Err(CreateTopicsPlanError::MixedReplicaPlacement);
    }
    if assignments.is_empty() {
        return Err(CreateTopicsPlanError::EmptyManualAssignments);
    }
    if assignments.len() > CREATE_TOPICS_MAX_MANUAL_PARTITIONS_PER_TOPIC {
        return Err(CreateTopicsPlanError::TooManyManualPartitions);
    }
    for (expected, assignment) in assignments.iter().enumerate() {
        if assignment.partition_index != expected as i32 {
            return Err(CreateTopicsPlanError::NonContiguousManualPartitions);
        }
        if assignment.broker_ids.is_empty() {
            return Err(CreateTopicsPlanError::EmptyManualReplicaSet);
        }
        if assignment.broker_ids.len() > CREATE_TOPICS_MAX_REPLICAS_PER_PARTITION {
            return Err(CreateTopicsPlanError::TooManyReplicas);
        }
        let mut brokers = BTreeSet::new();
        for broker_id in &assignment.broker_ids {
            if *broker_id < 0 {
                return Err(CreateTopicsPlanError::NegativeBrokerId);
            }
            if !brokers.insert(*broker_id) {
                return Err(CreateTopicsPlanError::DuplicateBrokerId);
            }
        }
        *broker_references = broker_references
            .checked_add(assignment.broker_ids.len())
            .ok_or(CreateTopicsPlanError::TooManyBrokerReferences)?;
        if *broker_references > CREATE_TOPICS_MAX_MANUAL_BROKER_REFERENCES {
            return Err(CreateTopicsPlanError::TooManyBrokerReferences);
        }
    }
    Ok(())
}

/// Invalid deterministic `CreateTopics` input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTopicsPlanError {
    /// Kafka cannot execute an empty `CreateTopics` batch.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// Topic names in one batch must be unique.
    DuplicateTopic,
    /// Automatic topic creation requires a positive partition count.
    InvalidPartitionCount,
    /// Replication factor must be positive or the broker-default sentinel.
    InvalidReplicationFactor,
    /// Automatic replication-factor intent cannot accompany manual placement.
    MixedReplicaPlacement,
    /// Manual placement must name at least one partition.
    EmptyManualAssignments,
    /// Manual placement exceeded the bounded partition count.
    TooManyManualPartitions,
    /// Manual partition indices must be caller-ordered and contiguous from zero.
    NonContiguousManualPartitions,
    /// Every manual partition must name at least one broker.
    EmptyManualReplicaSet,
    /// One manual partition exceeded the bounded replica count.
    TooManyReplicas,
    /// Broker IDs cannot be negative.
    NegativeBrokerId,
    /// One manual partition cannot name a broker twice.
    DuplicateBrokerId,
    /// Manual placement exceeded the request-wide broker-reference bound.
    TooManyBrokerReferences,
    /// Topic configuration names must not be empty.
    EmptyConfigName,
}

impl fmt::Display for CreateTopicsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBatch => "CreateTopics batch is empty",
            Self::EmptyTopicName => "CreateTopics topic name is empty",
            Self::DuplicateTopic => "CreateTopics batch contains a duplicate topic",
            Self::InvalidPartitionCount => "CreateTopics partition count is not positive",
            Self::InvalidReplicationFactor => "CreateTopics replication factor is invalid",
            Self::MixedReplicaPlacement => {
                "CreateTopics mixes automatic and manual replica placement"
            }
            Self::EmptyManualAssignments => "CreateTopics manual assignment set is empty",
            Self::TooManyManualPartitions => "CreateTopics has too many manual partitions",
            Self::NonContiguousManualPartitions => {
                "CreateTopics manual partition indexes are not contiguous from zero"
            }
            Self::EmptyManualReplicaSet => "CreateTopics manual replica set is empty",
            Self::TooManyReplicas => "CreateTopics manual partition has too many replicas",
            Self::NegativeBrokerId => "CreateTopics manual broker ID is negative",
            Self::DuplicateBrokerId => "CreateTopics manual broker ID is duplicated",
            Self::TooManyBrokerReferences => "CreateTopics has too many manual broker references",
            Self::EmptyConfigName => "CreateTopics configuration name is empty",
        })
    }
}

impl std::error::Error for CreateTopicsPlanError {}
