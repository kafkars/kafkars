//! Canonical bounded request-reservation scenarios for `DescribeTopics`.

use super::{DescribeTopicsRequest, limits::DESCRIBE_TOPICS_RETAINED_BYTES};

#[test]
fn admission_reserves_bounded_topic_result_capacity_before_acceptance() {
    let one = DescribeTopicsRequest::new(vec!["orders".to_owned()])
        .retained_charge()
        .unwrap_or_else(|| panic!("one topic charge should fit"));
    let two = DescribeTopicsRequest::new(vec!["orders".to_owned(), "audit".to_owned()])
        .retained_charge()
        .unwrap_or_else(|| panic!("two topic charge should fit"));
    assert!(one >= 128 * 1024);
    assert!(two >= one + 128 * 1024);
}

#[test]
fn all_topic_request_reserves_the_complete_host_envelope() {
    assert_eq!(
        DescribeTopicsRequest::all(false).retained_charge(),
        Some(DESCRIBE_TOPICS_RETAINED_BYTES)
    );
    assert_eq!(
        DescribeTopicsRequest::all(true)
            .into_plan()
            .unwrap_or_else(|error| panic!("all-topic plan should be valid: {error}")),
        kafka_client_core::DescribeTopicsPlan::all(true)
    );
}
