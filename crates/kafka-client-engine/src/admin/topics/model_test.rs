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

#[test]
fn topic_id_request_reserves_each_result_and_preserves_exact_ids() {
    let first = [1; 16];
    let second = [2; 16];
    let request = DescribeTopicsRequest::by_ids(vec![first, second]);
    let charge = request
        .retained_charge()
        .unwrap_or_else(|| panic!("topic-ID request charge should fit"));
    assert!(charge >= 2 * 128 * 1024);
    assert_eq!(
        request
            .into_plan()
            .unwrap_or_else(|error| panic!("topic-ID plan should be valid: {error}"))
            .selection(),
        &kafka_client_core::DescribeTopicsSelection::Ids(vec![first, second])
    );
}

#[test]
fn authorized_operations_are_explicit_and_default_false() {
    let default = DescribeTopicsRequest::new(vec!["orders".to_owned()])
        .into_plan()
        .unwrap_or_else(|error| panic!("default plan: {error}"));
    let requested = DescribeTopicsRequest::by_ids(vec![[1; 16]])
        .with_authorized_operations(true)
        .into_plan()
        .unwrap_or_else(|error| panic!("authorized plan: {error}"));
    assert!(!default.include_authorized_operations());
    assert!(requested.include_authorized_operations());
}

#[test]
fn all_topic_options_compose_without_call_order_changing_intent() {
    let expected =
        kafka_client_core::DescribeTopicsPlan::all(true).with_authorized_operations(true);
    let authorized_then_internal = DescribeTopicsRequest::all(false)
        .with_authorized_operations(true)
        .with_include_internal(true)
        .into_plan()
        .unwrap_or_else(|error| panic!("authorized then internal plan: {error}"));
    let internal_then_authorized = DescribeTopicsRequest::all(false)
        .with_include_internal(true)
        .with_authorized_operations(true)
        .into_plan()
        .unwrap_or_else(|error| panic!("internal then authorized plan: {error}"));

    assert_eq!(authorized_then_internal, expected);
    assert_eq!(internal_then_authorized, expected);
}
