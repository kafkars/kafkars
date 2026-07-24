//! Canonical bounded request-reservation scenarios for `DescribeTopics`.

use super::DescribeTopicsRequest;

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
