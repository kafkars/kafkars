//! Generated v0 contract, caller-order request, validation, and capacity scenarios.

use kafka_client_core::{
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS, DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS,
};
use kafka_wire::{DescribeTopicPartitionsRequest, KafkaMessage, KafkaRequest};
use kafka_wire_core::ApiVersion;

use super::{
    DescribeTopicPartitionsRequestCursor, DescribeTopicPartitionsRequestFailure,
    DescribeTopicPartitionsRequestPlan, describe_topic_partitions_request,
};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn generated_contract_is_flexible_api_75_v0_only() {
    assert_eq!(
        <DescribeTopicPartitionsRequest as KafkaRequest>::API_KEY.value(),
        75
    );
    assert!(DescribeTopicPartitionsRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(0)));
    assert!(!DescribeTopicPartitionsRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(1)));
    assert!(DescribeTopicPartitionsRequest::is_flexible(
        ApiVersion::new(0)
    ));
}

#[test]
fn request_preserves_caller_order_limit_and_optional_cursor() {
    let topics = vec!["zeta".to_owned(), "alpha".to_owned()];
    let cursor = DescribeTopicPartitionsRequestCursor::new("zeta", 7);
    let request = build(&topics, 32_768, Some(cursor), LIMIT)
        .unwrap_or_else(|error| panic!("valid request: {error:?}"));

    assert_eq!(
        request
            .topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>(),
        ["zeta", "alpha"]
    );
    assert_eq!(request.response_partition_limit, 32_768);
    let cursor = request.cursor.as_ref().expect("cursor");
    assert_eq!(cursor.topic_name.as_str(), "zeta");
    assert_eq!(cursor.partition_index, 7);
    assert!(request.unknown_tagged_fields.is_empty());
}

#[test]
fn request_rejects_identity_limit_cursor_and_capacity_failures() {
    assert_eq!(
        build(&[], 1, None, LIMIT),
        Err(DescribeTopicPartitionsRequestFailure::EmptyTopics)
    );
    let duplicate = vec!["orders".to_owned(), "orders".to_owned()];
    assert_eq!(
        build(&duplicate, 1, None, LIMIT),
        Err(DescribeTopicPartitionsRequestFailure::DuplicateTopic)
    );
    let one = vec!["orders".to_owned()];
    for actual in [0, DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS + 1] {
        assert_eq!(
            build(&one, actual, None, LIMIT),
            Err(
                DescribeTopicPartitionsRequestFailure::InvalidResponsePartitionLimit {
                    actual,
                    max: DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS,
                }
            )
        );
    }
    assert_eq!(
        build(
            &one,
            1,
            Some(DescribeTopicPartitionsRequestCursor::new("audit", 0)),
            LIMIT,
        ),
        Err(DescribeTopicPartitionsRequestFailure::CursorTopicNotRequested)
    );
    assert_eq!(
        build(
            &one,
            1,
            Some(DescribeTopicPartitionsRequestCursor::new("orders", -1)),
            LIMIT,
        ),
        Err(DescribeTopicPartitionsRequestFailure::NegativeCursorPartition { actual: -1 })
    );
    assert!(matches!(
        build(&one, 1, None, 0),
        Err(DescribeTopicPartitionsRequestFailure::RetainedBytes { .. })
    ));
    let too_many = (0..=DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS)
        .map(|index| format!("topic-{index}"))
        .collect::<Vec<_>>();
    assert!(matches!(
        build(&too_many, 1, None, LIMIT),
        Err(DescribeTopicPartitionsRequestFailure::TooManyTopics { .. })
    ));
}

fn build(
    topics: &[String],
    limit: u32,
    cursor: Option<DescribeTopicPartitionsRequestCursor<'_>>,
    retained_limit: usize,
) -> Result<DescribeTopicPartitionsRequest, DescribeTopicPartitionsRequestFailure> {
    describe_topic_partitions_request(
        DescribeTopicPartitionsRequestPlan::new(topics, limit, cursor),
        retained_limit,
    )
}
