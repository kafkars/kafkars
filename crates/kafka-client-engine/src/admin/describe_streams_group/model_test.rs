//! Inert and canonical API-89 request scenarios.

use super::{DescribeStreamsGroupRequest, DescribeStreamsGroupsRequest};

#[test]
fn request_canonicalizes_group_and_preserves_expansion_intent() {
    let request = DescribeStreamsGroupRequest::new(oversized("payments-streams"))
        .with_authorized_operations(true)
        .with_topology_description(true)
        .canonicalize();

    assert!(request.storage_is_canonical());
    assert_eq!(
        request.into_parts(),
        ("payments-streams".to_owned(), true, true)
    );
}

#[test]
fn invalid_intent_remains_inert_until_core_plan_conversion() {
    let (group_id, include_authorized, include_topology) =
        DescribeStreamsGroupRequest::new(String::new())
            .canonicalize()
            .into_parts();
    assert!(
        kafka_client_core::DescribeStreamsGroupPlan::new(
            group_id,
            include_authorized,
            include_topology,
        )
        .is_err()
    );
}

#[test]
fn batch_request_canonicalizes_caller_order_and_preserves_expansion_intent() {
    let request = DescribeStreamsGroupsRequest::new(vec![oversized("orders"), oversized("audit")])
        .with_authorized_operations(true)
        .with_topology_description(true)
        .canonicalize();

    assert!(request.storage_is_canonical());
    assert_eq!(
        request.into_parts(),
        (vec!["orders".to_owned(), "audit".to_owned()], true, true)
    );
}

#[test]
fn invalid_batch_intent_remains_inert_until_core_plan_conversion() {
    let (group_ids, include_authorized, include_topology) =
        DescribeStreamsGroupsRequest::new(vec!["orders".to_owned(), "orders".to_owned()])
            .canonicalize()
            .into_parts();
    assert!(
        kafka_client_core::DescribeStreamsGroupPlan::new_batch(
            group_ids,
            include_authorized,
            include_topology,
        )
        .is_err()
    );
}

fn oversized(value: &str) -> String {
    let mut owned = String::with_capacity(128);
    owned.push_str(value);
    owned
}
