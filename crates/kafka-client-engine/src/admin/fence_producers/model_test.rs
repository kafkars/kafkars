//! Scenarios for inert engine Admin `FenceProducers` requests.

use super::AdminFenceProducersRequest;

#[test]
fn request_preserves_caller_order_until_core_validation() {
    let plan = AdminFenceProducersRequest::new(vec![
        "orders-writer".to_owned(),
        "audit-writer".to_owned(),
    ])
    .canonicalize()
    .into_plan()
    .unwrap_or_else(|error| panic!("valid fencing plan: {error}"));

    assert_eq!(plan.transactional_ids(), ["orders-writer", "audit-writer"]);
}

#[test]
fn invalid_scalar_facts_remain_inert_until_plan_conversion() {
    let request = AdminFenceProducersRequest::new(vec![String::new()]);
    assert!(request.into_plan().is_err());
}
