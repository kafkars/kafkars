//! Stable v1 topology-description graph returned by API-89.

/// Raw-preserving v1 availability status paired with the nullable description.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopologyDescriptionStatus(i8);

impl DescribeStreamsGroupTopologyDescriptionStatus {
    /// Creates one exact broker status, including future values.
    pub const fn new(raw: i8) -> Self {
        Self(raw)
    }

    /// Returns Kafka's exact signed status code.
    pub const fn raw(self) -> i8 {
        self.0
    }
}

/// One processing node in a topology-description graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopologyDescriptionNode {
    name: String,
    node_type: i8,
    source_topics: Vec<String>,
    sink_topic: Option<String>,
    stores: Vec<String>,
    successors: Vec<String>,
}

impl DescribeStreamsGroupTopologyDescriptionNode {
    /// Creates one protocol-normalized node.
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

    /// Returns the node identity used for deterministic ordering.
    pub fn name(&self) -> &str {
        &self.name
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
    subtopology_id: String,
    nodes: Vec<DescribeStreamsGroupTopologyDescriptionNode>,
}

impl DescribeStreamsGroupTopologyDescriptionSubtopology {
    /// Creates one protocol-normalized described subtopology.
    pub const fn new(
        subtopology_id: String,
        nodes: Vec<DescribeStreamsGroupTopologyDescriptionNode>,
    ) -> Self {
        Self {
            subtopology_id,
            nodes,
        }
    }

    /// Returns the subtopology identity used for ordering.
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Consumes this subtopology into exact parts.
    pub fn into_parts(self) -> (String, Vec<DescribeStreamsGroupTopologyDescriptionNode>) {
        (self.subtopology_id, self.nodes)
    }
}

/// One global store's source and processor node pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupTopologyDescriptionGlobalStore {
    source: DescribeStreamsGroupTopologyDescriptionNode,
    processor: DescribeStreamsGroupTopologyDescriptionNode,
}

impl DescribeStreamsGroupTopologyDescriptionGlobalStore {
    /// Creates one protocol-normalized global store.
    pub const fn new(
        source: DescribeStreamsGroupTopologyDescriptionNode,
        processor: DescribeStreamsGroupTopologyDescriptionNode,
    ) -> Self {
        Self { source, processor }
    }

    /// Returns the deterministic source and processor identity.
    pub fn identity(&self) -> (&str, &str) {
        (self.source.name(), self.processor.name())
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
    subtopologies: Vec<DescribeStreamsGroupTopologyDescriptionSubtopology>,
    global_stores: Vec<DescribeStreamsGroupTopologyDescriptionGlobalStore>,
}

impl DescribeStreamsGroupTopologyDescription {
    /// Creates one protocol-normalized topology description.
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
