//! Request topic, limit, cursor, uniqueness, and caller-order validation.

#![expect(
    clippy::expect_used,
    reason = "test fixtures require contextual construction failures"
)]

use super::{
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS, DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS,
    DescribeTopicPartitionsCursor, DescribeTopicPartitionsPlan, DescribeTopicPartitionsPlanError,
};

#[test]
fn plan_preserves_caller_order_and_one_explicit_cursor() {
    let cursor = DescribeTopicPartitionsCursor::new("zeta".to_owned(), 7).expect("valid cursor");
    let plan = DescribeTopicPartitionsPlan::new(
        vec!["zeta".to_owned(), "alpha".to_owned()],
        2_000,
        Some(cursor),
    )
    .expect("valid page plan");
    assert_eq!(plan.topics(), ["zeta", "alpha"]);
    assert_eq!(plan.response_partition_limit(), 2_000);
    let cursor = plan.cursor().expect("cursor");
    assert_eq!(cursor.topic_name(), "zeta");
    assert_eq!(cursor.partition_index(), 7);
}

#[test]
fn topic_batch_limit_and_unique_names_are_strict() {
    assert_eq!(
        DescribeTopicPartitionsPlan::new(Vec::new(), 1, None),
        Err(DescribeTopicPartitionsPlanError::EmptyTopics)
    );
    assert_eq!(
        DescribeTopicPartitionsPlan::new(vec!["orders".to_owned(), "orders".to_owned()], 1, None,),
        Err(DescribeTopicPartitionsPlanError::DuplicateTopic)
    );
    assert_eq!(
        DescribeTopicPartitionsPlan::new(vec![String::new()], 1, None,),
        Err(DescribeTopicPartitionsPlanError::EmptyTopicName)
    );
    let too_many = (0..=DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS)
        .map(|index| format!("topic-{index}"))
        .collect();
    assert_eq!(
        DescribeTopicPartitionsPlan::new(too_many, 1, None),
        Err(DescribeTopicPartitionsPlanError::TooManyTopics)
    );
}

#[test]
fn positive_response_limit_and_cursor_correlation_are_validated() {
    assert_eq!(
        DescribeTopicPartitionsPlan::new(vec!["orders".to_owned()], 0, None),
        Err(DescribeTopicPartitionsPlanError::ZeroResponsePartitionLimit)
    );
    assert_eq!(
        DescribeTopicPartitionsPlan::new(
            vec!["orders".to_owned()],
            DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS + 1,
            None,
        ),
        Err(DescribeTopicPartitionsPlanError::ResponsePartitionLimitTooLarge)
    );
    assert_eq!(
        DescribeTopicPartitionsCursor::new("orders".to_owned(), -1),
        Err(DescribeTopicPartitionsPlanError::NegativeCursorPartition)
    );
    let cursor = DescribeTopicPartitionsCursor::new("audit".to_owned(), 0).expect("valid cursor");
    assert_eq!(
        DescribeTopicPartitionsPlan::new(vec!["orders".to_owned()], 1, Some(cursor),),
        Err(DescribeTopicPartitionsPlanError::CursorTopicNotRequested)
    );
}
