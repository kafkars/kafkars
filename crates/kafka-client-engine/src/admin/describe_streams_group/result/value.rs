//! Stable API-89 topology, task, and scalar result values.

/// One exact key-value pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupKeyValue {
    pub(super) key: String,
    pub(super) value: String,
}

impl DescribeStreamsGroupKeyValue {
    /// Creates one exact pair.
    pub const fn new(key: String, value: String) -> Self {
        Self { key, value }
    }

    /// Consumes this pair into exact parts.
    pub fn into_parts(self) -> (String, String) {
        (self.key, self.value)
    }
}

/// One streams topology topic and its creation policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopicInfo {
    pub(super) name: String,
    pub(super) partitions: i32,
    pub(super) replication_factor: i16,
    pub(super) configs: Vec<DescribeStreamsGroupKeyValue>,
}

impl DescribeStreamsGroupTopicInfo {
    /// Creates one exact topic description.
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
    pub(super) subtopology_id: String,
    pub(super) source_topics: Vec<String>,
    pub(super) repartition_sink_topics: Vec<String>,
    pub(super) state_changelog_topics: Vec<DescribeStreamsGroupTopicInfo>,
    pub(super) repartition_source_topics: Vec<DescribeStreamsGroupTopicInfo>,
}

impl DescribeStreamsGroupSubtopology {
    /// Creates one exact initialized subtopology.
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
    pub(super) epoch: i32,
    pub(super) subtopologies: Option<Vec<DescribeStreamsGroupSubtopology>>,
}

impl DescribeStreamsGroupTopology {
    /// Creates one exact initialized topology.
    pub const fn new(
        epoch: i32,
        subtopologies: Option<Vec<DescribeStreamsGroupSubtopology>>,
    ) -> Self {
        Self {
            epoch,
            subtopologies,
        }
    }

    /// Consumes this topology into exact parts.
    pub fn into_parts(self) -> (i32, Option<Vec<DescribeStreamsGroupSubtopology>>) {
        (self.epoch, self.subtopologies)
    }
}

/// Optional interactive-query endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupEndpoint {
    pub(super) host: String,
    pub(super) port: u16,
}

impl DescribeStreamsGroupEndpoint {
    /// Creates one exact endpoint.
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
    pub(super) subtopology_id: String,
    pub(super) partition: i32,
    pub(super) offset: i64,
}

impl DescribeStreamsGroupTaskOffset {
    /// Creates one exact task offset.
    pub const fn new(subtopology_id: String, partition: i32, offset: i64) -> Self {
        Self {
            subtopology_id,
            partition,
            offset,
        }
    }

    /// Consumes this task offset into exact parts.
    pub fn into_parts(self) -> (String, i32, i64) {
        (self.subtopology_id, self.partition, self.offset)
    }
}

/// One subtopology's assigned task partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTaskIds {
    pub(super) subtopology_id: String,
    pub(super) partitions: Vec<i32>,
}

impl DescribeStreamsGroupTaskIds {
    /// Creates one exact task partition group.
    pub const fn new(subtopology_id: String, partitions: Vec<i32>) -> Self {
        Self {
            subtopology_id,
            partitions,
        }
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
    reason = "the field names preserve Kafka's three distinct task classes"
)]
pub struct DescribeStreamsGroupAssignment {
    pub(super) active_tasks: Vec<DescribeStreamsGroupTaskIds>,
    pub(super) standby_tasks: Vec<DescribeStreamsGroupTaskIds>,
    pub(super) warmup_tasks: Vec<DescribeStreamsGroupTaskIds>,
}

impl DescribeStreamsGroupAssignment {
    /// Creates one exact task assignment.
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
