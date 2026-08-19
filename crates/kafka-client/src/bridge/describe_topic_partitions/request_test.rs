//! Public-to-engine API-key 75 page-intent translation scenarios.
#![expect(
    clippy::expect_used,
    reason = "the test requires both serialized topic markers"
)]

use super::DescribeTopicPartitionsAdminRequest;

#[test]
fn translation_preserves_topic_order_limit_and_explicit_cursor() {
    let request = DescribeTopicPartitionsAdminRequest::new(
        vec!["orders".to_owned(), "audit".to_owned()],
        17,
        Some(("audit".to_owned(), 9)),
    );
    let engine = format!("{:?}", request.into_engine());

    let orders = engine.find("\"orders\"").expect("orders topic");
    let audit = engine.find("\"audit\"").expect("audit topic");
    assert!(orders < audit);
    assert!(engine.contains("response_partition_limit: 17"));
    assert!(engine.contains("partition_index: 9"));
}

#[test]
fn invalid_limit_and_cursor_remain_inert_for_engine_boundary_validation() {
    let request =
        DescribeTopicPartitionsAdminRequest::new(Vec::new(), 0, Some((String::new(), -1)));
    let engine = format!("{:?}", request.into_engine());

    assert!(engine.contains("response_partition_limit: 0"));
    assert!(engine.contains("topic_name: \"\""));
    assert!(engine.contains("partition_index: -1"));
}
