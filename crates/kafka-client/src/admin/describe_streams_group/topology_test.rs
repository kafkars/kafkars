//! Initialized StreamsGroup topology value tests.

use super::{
    StreamsGroupKeyValue, StreamsGroupSubtopology, StreamsGroupTopicInfo, StreamsGroupTopology,
};

#[test]
fn topology_preserves_nullable_subtopologies_and_topic_facts() {
    assert_eq!(StreamsGroupTopology::new(4, None).subtopologies(), None);

    let topic = StreamsGroupTopicInfo::new(
        "orders-repartition".to_owned(),
        6,
        3,
        vec![StreamsGroupKeyValue::new(
            "cleanup.policy".to_owned(),
            "delete".to_owned(),
        )],
    );
    let topology = StreamsGroupTopology::new(
        5,
        Some(vec![StreamsGroupSubtopology::new(
            "sub-a".to_owned(),
            vec!["orders".to_owned()],
            vec!["orders-repartition".to_owned()],
            Vec::new(),
            vec![topic],
        )]),
    );

    assert_eq!(topology.epoch(), 5);
    let subtopology = &topology.subtopologies().unwrap_or_default()[0];
    assert_eq!(subtopology.subtopology_id(), "sub-a");
    assert_eq!(subtopology.source_topics(), ["orders"]);
    assert_eq!(
        subtopology.repartition_sink_topics(),
        ["orders-repartition"]
    );
    assert!(subtopology.state_changelog_topics().is_empty());
    let topic = &subtopology.repartition_source_topics()[0];
    assert_eq!(topic.name(), "orders-repartition");
    assert_eq!(topic.partitions(), 6);
    assert_eq!(topic.replication_factor(), 3);
    assert_eq!(topic.configs()[0].key(), "cleanup.policy");
}
