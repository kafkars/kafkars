//! Scenarios for inert group-offset request representation.

use super::{ListConsumerGroupOffsetsRequest, ListConsumerGroupsOffsetsRequest};

#[test]
fn request_preserves_group_and_stability_until_core_validation() {
    let request = ListConsumerGroupOffsetsRequest::new("payments".to_owned(), true).canonicalize();
    assert!(request.storage_is_canonical());

    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid group-offset plan: {error}"));
    assert_eq!(plan.group_id(), "payments");
    assert!(plan.require_stable());
}

#[test]
fn invalid_group_remains_inert_until_plan_conversion() {
    let request = ListConsumerGroupOffsetsRequest::new(String::new(), false);
    assert!(request.into_plan().is_err());
}

#[test]
fn plural_request_canonicalizes_all_group_storage_and_preserves_order() {
    let request = ListConsumerGroupsOffsetsRequest::new(
        vec![oversized("z-readers"), oversized("a-readers")],
        true,
    )
    .canonicalize();
    assert!(request.storage_is_canonical());

    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid plural request: {error}"));
    assert_eq!(plan.group_ids(), ["z-readers", "a-readers"]);
    assert!(plan.require_stable());
}

fn oversized(value: &str) -> String {
    let mut text = String::with_capacity(value.len() + 64);
    text.push_str(value);
    text
}
