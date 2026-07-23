//! Public topic-builder ownership scenarios.

use super::NewTopic;

#[test]
fn fluent_topic_values_preserve_ordered_configuration() {
    let topic = NewTopic::new("orders", 24)
        .replication_factor(3)
        .config("cleanup.policy", "compact")
        .config("min.insync.replicas", "2");

    assert_eq!(topic.name(), "orders");
    assert_eq!(topic.partitions(), 24);
    assert_eq!(topic.requested_replication_factor(), 3);
    let (_name, _partitions, _replication, configs) = topic.into_parts();
    assert_eq!(
        configs,
        [
            ("cleanup.policy".to_owned(), "compact".to_owned()),
            ("min.insync.replicas".to_owned(), "2".to_owned()),
        ]
    );
}
