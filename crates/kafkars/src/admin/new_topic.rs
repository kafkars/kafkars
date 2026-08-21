//! Stable Rust construction of automatic or manually placed topic creation.

/// One explicit partition-to-broker assignment for a newly created topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicReplicaAssignment {
    partition_index: i32,
    broker_ids: Vec<i32>,
}

impl TopicReplicaAssignment {
    /// Creates one caller-ordered manual partition placement.
    pub fn new<I>(partition_index: i32, broker_ids: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            partition_index,
            broker_ids: broker_ids.into_iter().collect(),
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

    pub(crate) fn into_parts(self) -> (i32, Vec<i32>) {
        (self.partition_index, self.broker_ids)
    }
}

/// Replica-placement intent for one newly created topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NewTopicPlacement {
    /// Kafka chooses replica placement from a partition count and replication factor.
    Automatic {
        /// Requested positive partition count.
        partitions: i32,
        /// Requested positive replication factor, or Kafka's `-1` default sentinel.
        replication_factor: i16,
    },
    /// The caller names every new partition and its ordered replicas.
    Manual {
        /// Caller-ordered partition assignments.
        assignments: Vec<TopicReplicaAssignment>,
    },
}

/// Topic creation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    name: String,
    placement: NewTopicPlacement,
    mixed_replication_factor: Option<i16>,
    configs: Vec<(String, String)>,
}

impl NewTopic {
    /// Creates a topic request with explicit partition count.
    pub fn new(name: impl Into<String>, partitions: i32) -> Self {
        Self {
            name: name.into(),
            placement: NewTopicPlacement::Automatic {
                partitions,
                replication_factor: -1,
            },
            mixed_replication_factor: None,
            configs: Vec::new(),
        }
    }

    /// Creates a topic whose every partition has an explicit replica placement.
    pub fn with_replica_assignments<I>(name: impl Into<String>, assignments: I) -> Self
    where
        I: IntoIterator<Item = TopicReplicaAssignment>,
    {
        Self {
            name: name.into(),
            placement: NewTopicPlacement::Manual {
                assignments: assignments.into_iter().collect(),
            },
            mixed_replication_factor: None,
            configs: Vec::new(),
        }
    }

    /// Sets the desired replication factor.
    ///
    /// Applying this to manual placement retains the conflicting intent so
    /// `submit()` can reject it as definitely unsent rather than silently
    /// discarding either setting.
    #[must_use]
    pub const fn replication_factor(mut self, replication_factor: i16) -> Self {
        match &mut self.placement {
            NewTopicPlacement::Automatic {
                replication_factor: requested,
                ..
            } => *requested = replication_factor,
            NewTopicPlacement::Manual { .. } => {
                self.mixed_replication_factor = Some(replication_factor);
            }
        }
        self
    }

    /// Appends one named topic configuration.
    #[must_use]
    pub fn config(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((name.into(), value.into()));
        self
    }

    /// Returns the requested topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the requested partition count.
    pub const fn partitions(&self) -> i32 {
        match &self.placement {
            NewTopicPlacement::Automatic { partitions, .. } => *partitions,
            NewTopicPlacement::Manual { .. } => -1,
        }
    }

    /// Returns the requested replication factor or Kafka's default sentinel.
    pub const fn requested_replication_factor(&self) -> i16 {
        match &self.placement {
            NewTopicPlacement::Automatic {
                replication_factor, ..
            } => *replication_factor,
            NewTopicPlacement::Manual { .. } => match self.mixed_replication_factor {
                Some(replication_factor) => replication_factor,
                None => -1,
            },
        }
    }

    /// Returns explicit automatic or manual replica-placement intent.
    pub const fn placement(&self) -> &NewTopicPlacement {
        &self.placement
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        NewTopicPlacement,
        Option<i16>,
        Vec<(String, String)>,
    ) {
        (
            self.name,
            self.placement,
            self.mixed_replication_factor,
            self.configs,
        )
    }
}
