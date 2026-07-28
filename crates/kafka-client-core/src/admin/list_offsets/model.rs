//! Validated caller-ordered intent for one Admin `ListOffsets` query.

use core::fmt;
use std::collections::BTreeSet;

use crate::ReadIsolation;

const MAX_TOPIC_NAME_BYTES: usize = 249;

/// Stable offset-selection policy for one topic-partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminListOffsetSpec {
    /// Select the earliest available offset.
    Earliest,
    /// Select the latest available offset.
    Latest,
    /// Select the record offset carrying the greatest timestamp.
    MaxTimestamp,
    /// Select the local-log start offset.
    EarliestLocal,
    /// Select the greatest offset already retained in tiered storage.
    LatestTiered,
    /// Select the earliest offset not yet uploaded to tiered storage.
    EarliestPendingUpload,
    /// Select the earliest offset whose record timestamp is at least this value.
    Timestamp(i64),
}

/// One caller-ordered topic-partition and its offset-selection policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminListOffsetTarget {
    topic: String,
    partition: i32,
    spec: AdminListOffsetSpec,
    current_leader_epoch: Option<i32>,
}

impl AdminListOffsetTarget {
    /// Creates one target for validation by the enclosing request plan.
    pub const fn new(topic: String, partition: i32, spec: AdminListOffsetSpec) -> Self {
        Self {
            topic,
            partition,
            spec,
            current_leader_epoch: None,
        }
    }

    /// Retains an optional leader-epoch fence for validation by the enclosing request plan.
    pub const fn with_current_leader_epoch(mut self, current_leader_epoch: Option<i32>) -> Self {
        self.current_leader_epoch = current_leader_epoch;
        self
    }

    /// Returns the exact UTF-8 topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the requested offset-selection policy.
    pub const fn spec(&self) -> AdminListOffsetSpec {
        self.spec
    }

    /// Returns the optional nonnegative leader-epoch fence.
    pub const fn current_leader_epoch(&self) -> Option<i32> {
        self.current_leader_epoch
    }

    /// Returns the earliest API-key 2 version representing this target and isolation policy.
    pub const fn minimum_api_version(&self, read_isolation: ReadIsolation) -> i16 {
        let selector_minimum = match self.spec {
            AdminListOffsetSpec::Earliest
            | AdminListOffsetSpec::Latest
            | AdminListOffsetSpec::Timestamp(_) => 1,
            AdminListOffsetSpec::MaxTimestamp => 7,
            AdminListOffsetSpec::EarliestLocal => 8,
            AdminListOffsetSpec::LatestTiered => 9,
            AdminListOffsetSpec::EarliestPendingUpload => 11,
        };
        let isolation_minimum =
            if matches!(read_isolation, ReadIsolation::ReadCommitted) && selector_minimum < 2 {
                2
            } else {
                selector_minimum
            };
        if self.current_leader_epoch.is_some() && isolation_minimum < 4 {
            4
        } else {
            isolation_minimum
        }
    }
}

/// Validated intent for one bounded Admin `ListOffsets` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminListOffsetsPlan {
    targets: Vec<AdminListOffsetTarget>,
    read_isolation: ReadIsolation,
}

impl AdminListOffsetsPlan {
    /// Validates one nonempty caller-ordered set of unique topic-partitions.
    pub fn new(targets: Vec<AdminListOffsetTarget>) -> Result<Self, AdminListOffsetsPlanError> {
        Self::with_read_isolation(targets, ReadIsolation::ReadUncommitted)
    }

    /// Validates targets and retains one immutable read-isolation policy.
    pub fn with_read_isolation(
        targets: Vec<AdminListOffsetTarget>,
        read_isolation: ReadIsolation,
    ) -> Result<Self, AdminListOffsetsPlanError> {
        if targets.is_empty() {
            return Err(AdminListOffsetsPlanError::EmptyTargetBatch);
        }
        let mut identities = BTreeSet::new();
        for target in &targets {
            validate_target(target)?;
            if !identities.insert((target.topic.as_str(), target.partition)) {
                return Err(AdminListOffsetsPlanError::DuplicateTopicPartition);
            }
        }
        Ok(Self {
            targets,
            read_isolation,
        })
    }

    /// Returns targets in exact caller order.
    pub fn targets(&self) -> &[AdminListOffsetTarget] {
        &self.targets
    }

    /// Returns the immutable visibility policy applied to every target.
    pub const fn read_isolation(&self) -> ReadIsolation {
        self.read_isolation
    }
}

fn validate_target(target: &AdminListOffsetTarget) -> Result<(), AdminListOffsetsPlanError> {
    if target.topic.is_empty() {
        return Err(AdminListOffsetsPlanError::EmptyTopicName);
    }
    if target.topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(AdminListOffsetsPlanError::TopicNameTooLong);
    }
    if target.partition < 0 {
        return Err(AdminListOffsetsPlanError::NegativePartition);
    }
    if matches!(target.spec, AdminListOffsetSpec::Timestamp(timestamp) if timestamp < 0) {
        return Err(AdminListOffsetsPlanError::NegativeTimestamp);
    }
    if target
        .current_leader_epoch
        .is_some_and(|current_leader_epoch| current_leader_epoch < 0)
    {
        return Err(AdminListOffsetsPlanError::NegativeCurrentLeaderEpoch);
    }
    Ok(())
}

/// Invalid deterministic Admin `ListOffsets` intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminListOffsetsPlanError {
    /// A query must contain at least one topic-partition.
    EmptyTargetBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// A topic name cannot fit Kafka's string domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// Caller timestamps must not overlap Kafka's negative selector sentinels.
    NegativeTimestamp,
    /// Present leader-epoch fences must be nonnegative.
    NegativeCurrentLeaderEpoch,
    /// One query cannot repeat a topic-partition identity.
    DuplicateTopicPartition,
}

impl fmt::Display for AdminListOffsetsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyTargetBatch => "Admin ListOffsets target batch is empty",
            Self::EmptyTopicName => "Admin ListOffsets topic is empty",
            Self::TopicNameTooLong => "Admin ListOffsets topic is too long",
            Self::NegativePartition => "Admin ListOffsets partition is negative",
            Self::NegativeTimestamp => "Admin ListOffsets timestamp is negative",
            Self::NegativeCurrentLeaderEpoch => {
                "Admin ListOffsets current leader epoch is negative"
            }
            Self::DuplicateTopicPartition => {
                "Admin ListOffsets contains a duplicate topic-partition"
            }
        })
    }
}

impl std::error::Error for AdminListOffsetsPlanError {}
