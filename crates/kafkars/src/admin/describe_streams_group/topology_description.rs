//! Stable optional full Streams topology-description graph.

/// Kafka's topology-description availability status.
///
/// Unknown values remain representable for forward compatibility.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamsGroupTopologyDescriptionStatus(i8);

impl StreamsGroupTopologyDescriptionStatus {
    /// The client did not request the full topology description.
    #[allow(non_upper_case_globals)]
    pub const NotRequested: Self = Self(0);
    /// Kafka has no stored topology description for this group.
    #[allow(non_upper_case_globals)]
    pub const NotStored: Self = Self(1);
    /// Kafka failed to fetch the stored topology description.
    #[allow(non_upper_case_globals)]
    pub const Error: Self = Self(2);
    /// The full topology description is present.
    #[allow(non_upper_case_globals)]
    pub const Available: Self = Self(3);

    /// Preserves a raw status code, including future values.
    pub const fn from_raw(value: i8) -> Self {
        Self(value)
    }

    /// Returns Kafka's exact signed status code.
    pub const fn as_raw(self) -> i8 {
        self.0
    }

    pub(crate) const fn from_engine(value: i8) -> Self {
        Self(value)
    }
}

/// Kafka's Streams topology node type.
///
/// Unknown values remain representable for forward compatibility.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamsGroupTopologyNodeType(i8);

impl StreamsGroupTopologyNodeType {
    /// A source node.
    #[allow(non_upper_case_globals)]
    pub const Source: Self = Self(1);
    /// A processor node.
    #[allow(non_upper_case_globals)]
    pub const Processor: Self = Self(2);
    /// A sink node.
    #[allow(non_upper_case_globals)]
    pub const Sink: Self = Self(3);

    /// Preserves a raw node-type code, including future values.
    pub const fn from_raw(value: i8) -> Self {
        Self(value)
    }

    /// Returns Kafka's exact signed node-type code.
    pub const fn as_raw(self) -> i8 {
        self.0
    }

    pub(crate) const fn from_engine(value: i8) -> Self {
        Self(value)
    }
}

/// One processing node in a full topology description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupTopologyNode {
    name: String,
    node_type: StreamsGroupTopologyNodeType,
    source_topics: Vec<String>,
    sink_topic: Option<String>,
    stores: Vec<String>,
    successors: Vec<String>,
}

impl StreamsGroupTopologyNode {
    pub(crate) const fn new(
        name: String,
        node_type: StreamsGroupTopologyNodeType,
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

    /// Returns the node name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the exact node type, including future codes.
    pub const fn node_type(&self) -> StreamsGroupTopologyNodeType {
        self.node_type
    }

    /// Returns source topics in deterministic UTF-8 byte order.
    pub fn source_topics(&self) -> &[String] {
        &self.source_topics
    }

    /// Returns the optional sink topic.
    pub fn sink_topic(&self) -> Option<&str> {
        self.sink_topic.as_deref()
    }

    /// Returns state-store names in deterministic UTF-8 byte order.
    pub fn stores(&self) -> &[String] {
        &self.stores
    }

    /// Returns successor node names in deterministic UTF-8 byte order.
    pub fn successors(&self) -> &[String] {
        &self.successors
    }
}

/// One subtopology in the optional full topology description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupTopologyDescriptionSubtopology {
    subtopology_id: String,
    nodes: Vec<StreamsGroupTopologyNode>,
}

impl StreamsGroupTopologyDescriptionSubtopology {
    pub(crate) const fn new(subtopology_id: String, nodes: Vec<StreamsGroupTopologyNode>) -> Self {
        Self {
            subtopology_id,
            nodes,
        }
    }

    /// Returns the subtopology identity.
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Returns processing nodes in node-name byte order.
    pub fn nodes(&self) -> &[StreamsGroupTopologyNode] {
        &self.nodes
    }
}

/// One global store's source and processor nodes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupTopologyGlobalStore {
    source: StreamsGroupTopologyNode,
    processor: StreamsGroupTopologyNode,
}

impl StreamsGroupTopologyGlobalStore {
    pub(crate) const fn new(
        source: StreamsGroupTopologyNode,
        processor: StreamsGroupTopologyNode,
    ) -> Self {
        Self { source, processor }
    }

    /// Returns the source node providing global-store data.
    pub const fn source(&self) -> &StreamsGroupTopologyNode {
        &self.source
    }

    /// Returns the processor node populating the global store.
    pub const fn processor(&self) -> &StreamsGroupTopologyNode {
        &self.processor
    }
}

/// Full topology graph returned by Kafka's topology-description plugin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupTopologyDescription {
    subtopologies: Vec<StreamsGroupTopologyDescriptionSubtopology>,
    global_stores: Vec<StreamsGroupTopologyGlobalStore>,
}

impl StreamsGroupTopologyDescription {
    pub(crate) const fn new(
        subtopologies: Vec<StreamsGroupTopologyDescriptionSubtopology>,
        global_stores: Vec<StreamsGroupTopologyGlobalStore>,
    ) -> Self {
        Self {
            subtopologies,
            global_stores,
        }
    }

    /// Returns full-description subtopologies ordered by identity.
    pub fn subtopologies(&self) -> &[StreamsGroupTopologyDescriptionSubtopology] {
        &self.subtopologies
    }

    /// Returns global stores in deterministic source/processor node order.
    pub fn global_stores(&self) -> &[StreamsGroupTopologyGlobalStore] {
        &self.global_stores
    }
}
