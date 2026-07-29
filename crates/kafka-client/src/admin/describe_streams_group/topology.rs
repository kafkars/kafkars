//! Stable initialized StreamsGroup topology metadata.

use super::StreamsGroupKeyValue;

/// One internal topic described by a StreamsGroup topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupTopicInfo {
    name: String,
    partitions: i32,
    replication_factor: i16,
    configs: Vec<StreamsGroupKeyValue>,
}

impl StreamsGroupTopicInfo {
    pub(crate) const fn new(
        name: String,
        partitions: i32,
        replication_factor: i16,
        configs: Vec<StreamsGroupKeyValue>,
    ) -> Self {
        Self {
            name,
            partitions,
            replication_factor,
            configs,
        }
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the configured partition count.
    ///
    /// Kafka may report zero when no specific partition count is enforced.
    pub const fn partitions(&self) -> i32 {
        self.partitions
    }

    /// Returns the configured replication factor.
    ///
    /// Kafka may report zero when the broker default applies.
    pub const fn replication_factor(&self) -> i16 {
        self.replication_factor
    }

    /// Returns topic configurations ordered by key bytes.
    pub fn configs(&self) -> &[StreamsGroupKeyValue] {
        &self.configs
    }
}

/// One initialized StreamsGroup subtopology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupSubtopology {
    subtopology_id: String,
    source_topics: Vec<String>,
    repartition_sink_topics: Vec<String>,
    state_changelog_topics: Vec<StreamsGroupTopicInfo>,
    repartition_source_topics: Vec<StreamsGroupTopicInfo>,
}

impl StreamsGroupSubtopology {
    pub(crate) const fn new(
        subtopology_id: String,
        source_topics: Vec<String>,
        repartition_sink_topics: Vec<String>,
        state_changelog_topics: Vec<StreamsGroupTopicInfo>,
        repartition_source_topics: Vec<StreamsGroupTopicInfo>,
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

    /// Returns source topics in deterministic UTF-8 byte order.
    pub fn source_topics(&self) -> &[String] {
        &self.source_topics
    }

    /// Returns repartition sink topics in deterministic UTF-8 byte order.
    pub fn repartition_sink_topics(&self) -> &[String] {
        &self.repartition_sink_topics
    }

    /// Returns state changelog topics in topic-name byte order.
    pub fn state_changelog_topics(&self) -> &[StreamsGroupTopicInfo] {
        &self.state_changelog_topics
    }

    /// Returns repartition source topics in topic-name byte order.
    pub fn repartition_source_topics(&self) -> &[StreamsGroupTopicInfo] {
        &self.repartition_source_topics
    }
}

/// The topology metadata currently initialized for a StreamsGroup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupTopology {
    epoch: i32,
    subtopologies: Option<Vec<StreamsGroupSubtopology>>,
}

impl StreamsGroupTopology {
    pub(crate) const fn new(
        epoch: i32,
        subtopologies: Option<Vec<StreamsGroupSubtopology>>,
    ) -> Self {
        Self {
            epoch,
            subtopologies,
        }
    }

    /// Returns the exact signed initialized-topology epoch.
    pub const fn epoch(&self) -> i32 {
        self.epoch
    }

    /// Returns initialized subtopologies, preserving Kafka's nullable state.
    ///
    /// `None` means the group is uninitialized or its source topics are
    /// missing or partitioned inconsistently. It is distinct from an empty
    /// initialized topology.
    pub fn subtopologies(&self) -> Option<&[StreamsGroupSubtopology]> {
        self.subtopologies.as_deref()
    }
}
