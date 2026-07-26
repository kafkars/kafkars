//! Scenarios for inert group-offset request representation.

use super::ListConsumerGroupOffsetsRequest;

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
