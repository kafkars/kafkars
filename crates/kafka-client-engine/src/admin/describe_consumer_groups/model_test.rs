//! Engine request canonicalization and validation scenarios.

use kafka_client_core::AdminDescribeConsumerGroupsScope;

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
    assert_eq!(plan.scope(), AdminDescribeConsumerGroupsScope::ModernFirst);
}

#[test]
fn classic_scope_reuses_the_same_canonical_request_and_plan_validation() {
    let Ok(plan) = DescribeConsumerGroupsRequest::new(vec!["beta".to_owned(), "alpha".to_owned()])
        .with_authorized_operations(true)
        .canonicalize()
        .into_plan_with_scope(AdminDescribeConsumerGroupsScope::ClassicOnly)
    else {
        panic!("valid classic plan expected");
    };
    assert_eq!(plan.groups(), ["beta", "alpha"]);
    assert!(plan.include_authorized_operations());
    assert_eq!(plan.scope(), AdminDescribeConsumerGroupsScope::ClassicOnly);
}

#[test]
fn invalid_request_category_remains_distinct() {
    assert!(
        DescribeConsumerGroupsRequest::new(Vec::new())
            .into_plan()
            .is_err()
    );
}
