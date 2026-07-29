//! Inert request, cursor, and core-validation scenarios.

use kafka_client_core::DescribeTopicPartitionsPlanError;

use super::{AdminDescribeTopicPartitionsCursor, AdminDescribeTopicPartitionsRequest};

#[test]
fn request_preserves_caller_order_limit_and_cursor() {
    let plan = AdminDescribeTopicPartitionsRequest::new(
        vec!["orders".to_owned(), "audit".to_owned()],
        2_000,
        Some(AdminDescribeTopicPartitionsCursor::new(
            "audit".to_owned(),
            7,
        )),
    )
    .canonicalize()
    .into_plan()
    .unwrap_or_else(|error| panic!("valid page plan: {error}"));

    assert_eq!(plan.topics(), ["orders", "audit"]);
    assert_eq!(plan.response_partition_limit(), 2_000);
    let cursor = plan.cursor().unwrap_or_else(|| panic!("cursor expected"));
    assert_eq!(cursor.topic_name(), "audit");
    assert_eq!(cursor.partition_index(), 7);
}

#[test]
fn request_and_cursor_remain_inert_until_plan_conversion() {
    let duplicate = AdminDescribeTopicPartitionsRequest::new(
        vec!["orders".to_owned(), "orders".to_owned()],
        1,
        None,
    )
    .into_plan();
    assert_eq!(
        duplicate,
        Err(DescribeTopicPartitionsPlanError::DuplicateTopic)
    );

    let negative_cursor = AdminDescribeTopicPartitionsRequest::new(
        vec!["orders".to_owned()],
        1,
        Some(AdminDescribeTopicPartitionsCursor::new(
            "orders".to_owned(),
            -1,
        )),
    )
    .into_plan();
    assert_eq!(
        negative_cursor,
        Err(DescribeTopicPartitionsPlanError::NegativeCursorPartition)
    );
}

#[test]
fn consuming_accessors_preserve_exact_request_values() {
    let request = AdminDescribeTopicPartitionsRequest::new(
        vec!["orders".to_owned()],
        17,
        Some(AdminDescribeTopicPartitionsCursor::new(
            "orders".to_owned(),
            9,
        )),
    );
    let (topics, limit, cursor) = request.into_parts();
    assert_eq!(topics, ["orders"]);
    assert_eq!(limit, 17);
    assert_eq!(
        cursor.map(AdminDescribeTopicPartitionsCursor::into_parts),
        Some(("orders".to_owned(), 9))
    );
}
