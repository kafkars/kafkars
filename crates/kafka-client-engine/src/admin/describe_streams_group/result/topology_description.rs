//! Stable API-89 v1 topology-description result values.

/// Raw-preserving v1 topology-description availability status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopologyDescriptionStatus {
    pub(super) raw: i8,
}

impl DescribeStreamsGroupTopologyDescriptionStatus {
    /// Creates one exact status, including future values.
    pub const fn new(raw: i8) -> Self {
        Self { raw }
    }

    /// Returns Kafka's exact signed status code.
    pub const fn raw(self) -> i8 {
        self.raw
    }
}

/// One processing node in a topology-description graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopologyDescriptionNode {
    pub(super) name: String,
    pub(super) node_type: i8,
    pub(super) source_topics: Vec<String>,
    pub(super) sink_topic: Option<String>,
    pub(super) stores: Vec<String>,
    pub(super) successors: Vec<String>,
}

impl DescribeStreamsGroupTopologyDescriptionNode {
    /// Creates one exact topology node.
    pub const fn new(
        name: String,
        node_type: i8,
        source_topics: Vec<String>,
        sink_topic: Option<String>,
        stores: Vec<String>,
        successors: Vec<String>,
    ) -> Self {
        Self {
            name,
            node_type,
            source_topics,
            sink_topic,
            stores,
            successors,
        }
    }

    /// Consumes this node into exact parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        String,
        i8,
        Vec<String>,
        Option<String>,
        Vec<String>,
        Vec<String>,
    ) {
        (
            self.name,
            self.node_type,
            self.source_topics,
            self.sink_topic,
            self.stores,
            self.successors,
        )
    }
}

/// One named subtopology in a v1 topology description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopologyDescriptionSubtopology {
    pub(super) subtopology_id: String,
    pub(super) nodes: Vec<DescribeStreamsGroupTopologyDescriptionNode>,
}

impl DescribeStreamsGroupTopologyDescriptionSubtopology {
    /// Creates one exact described subtopology.
    pub const fn new(
        subtopology_id: String,
        nodes: Vec<DescribeStreamsGroupTopologyDescriptionNode>,
    ) -> Self {
        Self {
            subtopology_id,
            nodes,
        }
    }

    /// Consumes this subtopology into exact parts.
    pub fn into_parts(self) -> (String, Vec<DescribeStreamsGroupTopologyDescriptionNode>) {
        (self.subtopology_id, self.nodes)
    }
}

/// One global store's source and processor node pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopologyDescriptionGlobalStore {
    pub(super) source: DescribeStreamsGroupTopologyDescriptionNode,
    pub(super) processor: DescribeStreamsGroupTopologyDescriptionNode,
}

impl DescribeStreamsGroupTopologyDescriptionGlobalStore {
    /// Creates one exact global store.
    pub const fn new(
        source: DescribeStreamsGroupTopologyDescriptionNode,
        processor: DescribeStreamsGroupTopologyDescriptionNode,
    ) -> Self {
        Self { source, processor }
    }

    /// Consumes this global store into exact nodes.
    pub fn into_parts(
        self,
    ) -> (
        DescribeStreamsGroupTopologyDescriptionNode,
        DescribeStreamsGroupTopologyDescriptionNode,
    ) {
        (self.source, self.processor)
    }
}

/// Complete v1 topology-description graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopologyDescription {
    pub(super) subtopologies: Vec<DescribeStreamsGroupTopologyDescriptionSubtopology>,
    pub(super) global_stores: Vec<DescribeStreamsGroupTopologyDescriptionGlobalStore>,
}

impl DescribeStreamsGroupTopologyDescription {
    /// Creates one exact topology description.
    pub const fn new(
        subtopologies: Vec<DescribeStreamsGroupTopologyDescriptionSubtopology>,
        global_stores: Vec<DescribeStreamsGroupTopologyDescriptionGlobalStore>,
    ) -> Self {
        Self {
            subtopologies,
            global_stores,
        }
    }

    /// Consumes this description into exact parts.
    pub fn into_parts(
        self,
    ) -> (
        Vec<DescribeStreamsGroupTopologyDescriptionSubtopology>,
        Vec<DescribeStreamsGroupTopologyDescriptionGlobalStore>,
    ) {
        (self.subtopologies, self.global_stores)
    }
}
