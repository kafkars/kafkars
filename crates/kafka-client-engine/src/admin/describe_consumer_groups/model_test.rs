//! Engine request canonicalization and validation scenarios.

use super::DescribeConsumerGroupsRequest;

#[test]
fn request_preserves_caller_order_and_explicit_authorization_intent() {
    let Ok(plan) = DescribeConsumerGroupsRequest::new(vec!["beta".to_owned(), "alpha".to_owned()])
        .with_authorized_operations(true)
        .canonicalize()
        .into_plan()
    else {
        panic!("valid plan expected");
    };
    assert_eq!(plan.groups(), ["beta", "alpha"]);
    assert!(plan.include_authorized_operations());
}

#[test]
fn invalid_request_category_remains_distinct() {
    assert!(
        DescribeConsumerGroupsRequest::new(Vec::new())
            .into_plan()
            .is_err()
    );
}
