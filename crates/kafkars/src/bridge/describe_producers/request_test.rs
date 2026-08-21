//! Public-to-engine Admin `DescribeProducers` request translation scenarios.
#![expect(
    clippy::expect_used,
    reason = "the test requires both serialized topic markers"
)]

use crate::{StartPosition, TopicPartition};

use super::DescribeProducersAdminRequest;

#[test]
fn translation_is_deferred_and_preserves_caller_order() {
    let request = DescribeProducersAdminRequest::new(vec![
        TopicPartition::new("orders", 2),
        TopicPartition::new("audit", 0),
    ])
    .with_broker_id(7);
    let engine = format!("{:?}", request.into_engine());

    let orders = engine.find("\"orders\"").expect("orders target");
    let audit = engine.find("\"audit\"").expect("audit target");
    assert!(orders < audit);
    assert!(engine.contains("broker_id: Some(7)"));
}

#[test]
fn assignment_only_start_position_is_preserved_as_invalid_input() {
    let request = DescribeProducersAdminRequest::new(vec![
        TopicPartition::new("orders", 2).start_at(StartPosition::End),
    ]);
    let engine = format!("{:?}", request.into_engine());

    assert!(engine.contains(&i32::MIN.to_string()));
}

#[test]
fn negative_broker_identity_crosses_inertly_for_submit_time_validation() {
    let request = DescribeProducersAdminRequest::new(vec![TopicPartition::new("orders", 2)])
        .with_broker_id(-1);
    let engine = format!("{:?}", request.into_engine());

    assert!(engine.contains("broker_id: Some(-1)"));
}
