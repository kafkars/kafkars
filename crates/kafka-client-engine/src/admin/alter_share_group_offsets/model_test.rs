//! Inert and canonical API-91 request scenarios.

use super::{AlterShareGroupOffset, AlterShareGroupOffsetsRequest};

#[test]
fn request_canonicalizes_owned_storage_and_preserves_caller_order() {
    let request = AlterShareGroupOffsetsRequest::new(
        oversized("payments-share"),
        vec![
            AlterShareGroupOffset::new(oversized("orders"), 1, 42),
            AlterShareGroupOffset::new(oversized("audit"), 0, 7),
        ],
    )
    .canonicalize();

    assert!(request.storage_is_canonical());
    let (group_id, changes) = request.into_parts();
    assert_eq!(group_id, "payments-share");
    assert_eq!(changes[0].topic(), "orders");
    assert_eq!(changes[0].partition(), 1);
    assert_eq!(changes[0].start_offset(), 42);
    assert_eq!(changes[1].topic(), "audit");
}

#[test]
fn invalid_intent_remains_inert_until_core_plan_conversion() {
    let request = AlterShareGroupOffsetsRequest::new(
        String::new(),
        vec![AlterShareGroupOffset::new("orders".to_owned(), 0, 7)],
    );
    let (group_id, changes) = request.canonicalize().into_parts();

    assert!(kafka_client_core::AlterShareGroupOffsetsPlan::new(group_id, changes).is_err());
}

fn oversized(value: &str) -> String {
    let mut owned = String::with_capacity(128);
    owned.push_str(value);
    owned
}
