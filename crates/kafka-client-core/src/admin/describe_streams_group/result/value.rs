//! Reusable generated-free API-89 topology, task, and scalar values.

/// One exact key-value pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupKeyValue {
    key: String,
    value: String,
}

impl DescribeStreamsGroupKeyValue {
    /// Creates one protocol-normalized pair.
    pub const fn new(key: String, value: String) -> Self {
        Self { key, value }
    }

    /// Returns the key used for deterministic ordering.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Consumes the pair into exact scalar parts.
    pub fn into_parts(self) -> (String, String) {
        (self.key, self.value)
    }
}

/// One streams topology topic and its creation policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopicInfo {
    name: String,
    partitions: i32,
    replication_factor: i16,
    configs: Vec<DescribeStreamsGroupKeyValue>,
}

impl DescribeStreamsGroupTopicInfo {
    /// Creates one protocol-normalized topic description.
    pub const fn new(
        name: String,
        partitions: i32,
        replication_factor: i16,
        configs: Vec<DescribeStreamsGroupKeyValue>,
    ) -> Self {
        Self {
            name,
            partitions,
            replication_factor,
            configs,
        }
    }

    /// Returns the topic name used for deterministic ordering.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Consumes this topic into exact parts.
    pub fn into_parts(self) -> (String, i32, i16, Vec<DescribeStreamsGroupKeyValue>) {
        (
            self.name,
            self.partitions,
            self.replication_factor,
            self.configs,
        )
    }
}

/// One initialized streams subtopology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupSubtopology {
    subtopology_id: String,
    source_topics: Vec<String>,
    repartition_sink_topics: Vec<String>,
    state_changelog_topics: Vec<DescribeStreamsGroupTopicInfo>,
    repartition_source_topics: Vec<DescribeStreamsGroupTopicInfo>,
}

impl DescribeStreamsGroupSubtopology {
    /// Creates one protocol-normalized subtopology.
    pub const fn new(
        subtopology_id: String,
        source_topics: Vec<String>,
        repartition_sink_topics: Vec<String>,
        state_changelog_topics: Vec<DescribeStreamsGroupTopicInfo>,
        repartition_source_topics: Vec<DescribeStreamsGroupTopicInfo>,
    ) -> Self {
        Self {
            subtopology_id,
            source_topics,
            repartition_sink_topics,
            state_changelog_topics,
            repartition_source_topics,
        }
    }

    /// Returns the stable subtopology identity.
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Consumes this subtopology into exact parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        String,
        Vec<String>,
        Vec<String>,
        Vec<DescribeStreamsGroupTopicInfo>,
        Vec<DescribeStreamsGroupTopicInfo>,
    ) {
        (
            self.subtopology_id,
            self.source_topics,
            self.repartition_sink_topics,
            self.state_changelog_topics,
            self.repartition_source_topics,
        )
    }
}

/// Current initialized streams topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopology {
    epoch: i32,
    subtopologies: Option<Vec<DescribeStreamsGroupSubtopology>>,
}

impl DescribeStreamsGroupTopology {
    /// Creates one protocol-normalized initialized topology.
    pub const fn new(
        epoch: i32,
        subtopologies: Option<Vec<DescribeStreamsGroupSubtopology>>,
    ) -> Self {
        Self {
            epoch,
            subtopologies,
        }
    }

    /// Returns the exact topology epoch.
    pub const fn epoch(&self) -> i32 {
        self.epoch
    }

    /// Consumes the topology into exact parts.
    pub fn into_parts(self) -> (i32, Option<Vec<DescribeStreamsGroupSubtopology>>) {
        (self.epoch, self.subtopologies)
    }
}

/// Optional interactive-query endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupEndpoint {
    host: String,
    port: u16,
}

impl DescribeStreamsGroupEndpoint {
    /// Creates one protocol-normalized endpoint.
    pub const fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }

    /// Consumes this endpoint into exact parts.
    pub fn into_parts(self) -> (String, u16) {
        (self.host, self.port)
    }
}

/// One changelog offset for a streams task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTaskOffset {
    subtopology_id: String,
    partition: i32,
    offset: i64,
}

impl DescribeStreamsGroupTaskOffset {
    /// Creates one protocol-normalized task offset.
    pub const fn new(subtopology_id: String, partition: i32, offset: i64) -> Self {
        Self {
            subtopology_id,
            partition,
            offset,
        }
    }

    /// Returns the composite identity used for deterministic ordering.
    pub fn identity(&self) -> (&str, i32) {
        (&self.subtopology_id, self.partition)
    }

    /// Consumes this task offset into exact parts.
    pub fn into_parts(self) -> (String, i32, i64) {
        (self.subtopology_id, self.partition, self.offset)
    }
}

/// One subtopology's assigned task partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTaskIds {
    subtopology_id: String,
    partitions: Vec<i32>,
}

impl DescribeStreamsGroupTaskIds {
    /// Creates one protocol-normalized task group.
    pub const fn new(subtopology_id: String, partitions: Vec<i32>) -> Self {
        Self {
            subtopology_id,
            partitions,
        }
    }

    /// Returns the stable subtopology identity.
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Returns deterministic nonnegative partitions.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    /// Consumes this task group into exact parts.
    pub fn into_parts(self) -> (String, Vec<i32>) {
        (self.subtopology_id, self.partitions)
    }
}

/// Current or target task assignment for one member.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_field_names,
    reason = "the task suffix mirrors Kafka's public assignment vocabulary"
)]
pub struct DescribeStreamsGroupAssignment {
    active_tasks: Vec<DescribeStreamsGroupTaskIds>,
    standby_tasks: Vec<DescribeStreamsGroupTaskIds>,
    warmup_tasks: Vec<DescribeStreamsGroupTaskIds>,
}

impl DescribeStreamsGroupAssignment {
    /// Creates one protocol-normalized assignment.
    pub const fn new(
        active_tasks: Vec<DescribeStreamsGroupTaskIds>,
        standby_tasks: Vec<DescribeStreamsGroupTaskIds>,
        warmup_tasks: Vec<DescribeStreamsGroupTaskIds>,
    ) -> Self {
        Self {
            active_tasks,
            standby_tasks,
            warmup_tasks,
        }
    }

    /// Consumes this assignment into exact task classes.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Vec<DescribeStreamsGroupTaskIds>,
        Vec<DescribeStreamsGroupTaskIds>,
        Vec<DescribeStreamsGroupTaskIds>,
    ) {
        (self.active_tasks, self.standby_tasks, self.warmup_tasks)
    }
}
