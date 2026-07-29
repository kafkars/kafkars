//! Inert and canonical API-77 request scenarios.

use super::{DescribeShareGroupRequest, DescribeShareGroupsRequest};

#[test]
fn request_canonicalizes_group_and_preserves_authorization_intent() {
    let request = DescribeShareGroupRequest::new(oversized("payments-share"))
        .with_authorized_operations(true)
        .canonicalize();

    assert!(request.storage_is_canonical());
    assert_eq!(request.into_parts(), ("payments-share".to_owned(), true));
}

#[test]
fn invalid_intent_remains_inert_until_core_plan_conversion() {
    let (group_id, include) = DescribeShareGroupRequest::new(String::new())
        .canonicalize()
        .into_parts();
    assert!(kafka_client_core::DescribeShareGroupPlan::new(group_id, include).is_err());
}

#[test]
fn batch_request_canonicalizes_storage_and_preserves_order_and_intent() {
    let request = DescribeShareGroupsRequest::new(vec![
        oversized("payments-share"),
        oversized("orders-share"),
    ])
    .with_authorized_operations(true)
    .canonicalize();

    assert!(request.storage_is_canonical());
    assert_eq!(
        request.into_parts(),
        (
            vec!["payments-share".to_owned(), "orders-share".to_owned()],
            true,
        )
    );
}

#[test]
fn invalid_batch_remains_inert_until_core_plan_conversion() {
    let (group_ids, include) = DescribeShareGroupsRequest::new(vec![
        "payments-share".to_owned(),
        "payments-share".to_owned(),
    ])
    .canonicalize()
    .into_parts();

    assert!(kafka_client_core::DescribeShareGroupPlan::new_batch(group_ids, include).is_err());
}

fn oversized(value: &str) -> String {
    let mut owned = String::with_capacity(128);
    owned.push_str(value);
    owned
}
