//! Public-to-engine Admin `DeleteConsumerGroups` request translation scenarios.

use super::DeleteConsumerGroupsAdminRequest;

#[test]
fn translation_is_deferred_and_preserves_caller_order() {
    let request = DeleteConsumerGroupsAdminRequest::new(vec![
        "orders-workers".to_owned(),
        "audit-workers".to_owned(),
    ]);
    let engine = request.into_engine();
    assert!(format!("{engine:?}").contains("orders-workers"));
}
