//! Exact single-target `DescribeProducers` request construction.

use kafka_client_core::AdminDescribeProducerTarget;

use super::describe_producers_request;

#[test]
fn request_contains_only_the_selected_topic_and_partition() {
    let target = target();
    let request = describe_producers_request(&target);

    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].name.as_str(), "audit-log");
    assert_eq!(request.topics[0].partition_indexes, vec![7]);
    assert!(request.topics[0].unknown_tagged_fields.is_empty());
    assert!(request.unknown_tagged_fields.is_empty());
}

fn target() -> AdminDescribeProducerTarget {
    AdminDescribeProducerTarget::new("audit-log".to_owned(), 7)
}
