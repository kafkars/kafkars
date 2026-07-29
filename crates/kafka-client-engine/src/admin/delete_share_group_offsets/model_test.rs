//! Inert and canonical API-92 request scenarios.

use super::DeleteShareGroupOffsetsRequest;

#[test]
fn request_canonicalizes_owned_storage_and_preserves_caller_order() {
    let request = DeleteShareGroupOffsetsRequest::new(
        oversized("payments-share"),
        vec![oversized("orders"), oversized("audit")],
    )
    .canonicalize();

    assert!(request.storage_is_canonical());
    let (group_id, topics) = request.into_parts();
    assert_eq!(group_id, "payments-share");
    assert_eq!(topics, ["orders", "audit"]);
}

#[test]
fn invalid_intent_remains_inert_until_core_plan_conversion() {
    let request = DeleteShareGroupOffsetsRequest::new(String::new(), vec!["orders".to_owned()]);
    let (group_id, topics) = request.canonicalize().into_parts();

    assert!(kafka_client_core::DeleteShareGroupOffsetsPlan::new(group_id, topics).is_err());
}

fn oversized(value: &str) -> String {
    let mut owned = String::with_capacity(128);
    owned.push_str(value);
    owned
}
