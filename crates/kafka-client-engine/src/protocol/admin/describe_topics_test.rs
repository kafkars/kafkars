//! Generated transient `DescribeTopics` request scenarios.

use kafka_wire_core::{ApiVersion, EncodeError, KafkaEncode, StrBytes};

use kafka_client_core::DescribeTopicsPlan;

use super::describe_topics::describe_topics_request;

#[test]
fn request_batches_names_without_auto_creation_or_authorization_queries() {
    let plan = DescribeTopicsPlan::new(vec!["orders".to_owned(), "audit".to_owned()])
        .unwrap_or_else(|error| panic!("valid named plan: {error}"));
    let request = describe_topics_request(&plan);
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
fn all_topic_request_is_nullable_and_never_requests_authorization_expansion() {
    let request = describe_topics_request(&DescribeTopicsPlan::all(false));
    assert_eq!(request.topics, None);
    assert!(!request.allow_auto_topic_creation);
    assert!(!request.include_cluster_authorized_operations);
    assert!(!request.include_topic_authorized_operations);
}

#[test]
fn brokers_older_than_v4_fail_locally_instead_of_auto_creating() {
    let plan = DescribeTopicsPlan::new(vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("valid named plan: {error}"));
    let request = describe_topics_request(&plan);
    assert!(matches!(
        request.encoded_len(ApiVersion::new(3)),
        Err(EncodeError::FieldNotRepresentable {
            field: "AllowAutoTopicCreation",
            ..
        })
    ));
    assert!(request.encoded_len(ApiVersion::new(4)).is_ok());
}

#[test]
fn all_topic_query_never_falls_back_for_older_metadata_versions() {
    let request = describe_topics_request(&DescribeTopicsPlan::all(true));
    for version in 0..=3 {
        assert!(matches!(
            request.encoded_len(ApiVersion::new(version)),
            Err(EncodeError::FieldNotRepresentable {
                field: "AllowAutoTopicCreation",
                ..
            })
        ));
    }
    assert!(request.encoded_len(ApiVersion::new(4)).is_ok());
    assert_eq!(request.topics, None);

    let mut generated_nullable_control = request;
    generated_nullable_control.allow_auto_topic_creation = true;
    for version in 1..=3 {
        assert!(
            generated_nullable_control
                .encoded_len(ApiVersion::new(version))
                .is_ok()
        );
    }
}
