//! Generated transient `DescribeTopics` request scenarios.

use kafka_wire_core::StrBytes;
use kafka_wire_core::{ApiVersion, EncodeError, KafkaEncode};

use super::describe_topics::describe_topics_request;

#[test]
fn request_batches_names_without_auto_creation_or_authorization_queries() {
    let request = describe_topics_request(["orders", "audit"]);
    let topics = request
        .topics
        .unwrap_or_else(|| panic!("name-based DescribeTopics must use an explicit topic array"));

    assert_eq!(topics.len(), 2);
    assert_eq!(
        topics[0].name.as_ref().map(StrBytes::as_str),
        Some("orders")
    );
    assert_eq!(topics[1].name.as_ref().map(StrBytes::as_str), Some("audit"));
    assert!(!request.allow_auto_topic_creation);
    assert!(!request.include_cluster_authorized_operations);
    assert!(!request.include_topic_authorized_operations);
}

#[test]
fn brokers_older_than_v4_fail_locally_instead_of_auto_creating() {
    let request = describe_topics_request(["orders"]);
    assert!(matches!(
        request.encoded_len(ApiVersion::new(3)),
        Err(EncodeError::FieldNotRepresentable {
            field: "AllowAutoTopicCreation",
            ..
        })
    ));
    assert!(request.encoded_len(ApiVersion::new(4)).is_ok());
}
