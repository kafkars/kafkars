//! Scenarios for inert engine Admin `DeleteConsumerGroups` requests.

use super::DeleteConsumerGroupsRequest;

#[test]
fn request_preserves_caller_order_until_core_validation() {
    let request = DeleteConsumerGroupsRequest::new(vec![
        "orders-workers".to_owned(),
        "audit-workers".to_owned(),
    ])
    .canonicalize();
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.targets()[0].group_id(), "orders-workers");
    assert_eq!(plan.targets()[1].group_id(), "audit-workers");
}

#[test]
fn invalid_group_ids_remain_inert_until_plan_conversion() {
    let request = DeleteConsumerGroupsRequest::new(vec![String::new()]);
    assert!(request.into_plan().is_err());
}
