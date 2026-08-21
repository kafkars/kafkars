//! Full Streams topology-description value tests.

use super::{
    StreamsGroupTopologyDescription, StreamsGroupTopologyDescriptionStatus,
    StreamsGroupTopologyDescriptionSubtopology, StreamsGroupTopologyGlobalStore,
    StreamsGroupTopologyNode, StreamsGroupTopologyNodeType,
};

fn node(name: &str, node_type: StreamsGroupTopologyNodeType) -> StreamsGroupTopologyNode {
    StreamsGroupTopologyNode::new(
        name.to_owned(),
        node_type,
        vec!["orders".to_owned()],
        Some("orders-out".to_owned()),
        vec!["counts".to_owned()],
        vec!["sink".to_owned()],
    )
}

#[test]
fn full_description_preserves_graph_and_future_numeric_codes() {
    assert_eq!(StreamsGroupTopologyDescriptionStatus::Available.as_raw(), 3);
    assert_eq!(
        StreamsGroupTopologyDescriptionStatus::from_raw(91).as_raw(),
        91
    );
    assert_eq!(StreamsGroupTopologyNodeType::Processor.as_raw(), 2);
    assert_eq!(StreamsGroupTopologyNodeType::from_raw(89).as_raw(), 89);

    let source = node("source", StreamsGroupTopologyNodeType::Source);
    let processor = node("processor", StreamsGroupTopologyNodeType::Processor);
    let description = StreamsGroupTopologyDescription::new(
        vec![StreamsGroupTopologyDescriptionSubtopology::new(
            "sub-a".to_owned(),
            vec![processor.clone()],
        )],
        vec![StreamsGroupTopologyGlobalStore::new(source, processor)],
    );

    let node = &description.subtopologies()[0].nodes()[0];
    assert_eq!(description.subtopologies()[0].subtopology_id(), "sub-a");
    assert_eq!(node.name(), "processor");
    assert_eq!(node.node_type(), StreamsGroupTopologyNodeType::Processor);
    assert_eq!(node.source_topics(), ["orders"]);
    assert_eq!(node.sink_topic(), Some("orders-out"));
    assert_eq!(node.stores(), ["counts"]);
    assert_eq!(node.successors(), ["sink"]);
    assert_eq!(description.global_stores()[0].source().name(), "source");
    assert_eq!(
        description.global_stores()[0].processor().name(),
        "processor"
    );
}
