//! Inert and canonical offset-deletion request scenarios.

use super::{DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsRequest};

#[test]
fn request_canonicalizes_owned_storage_and_preserves_caller_order() {
    let request = DeleteConsumerGroupOffsetsRequest::new(
        oversized("payments"),
        vec![
            DeleteConsumerGroupOffsetTarget::new(oversized("orders"), 2),
            DeleteConsumerGroupOffsetTarget::new(oversized("audit"), 0),
        ],
    )
    .canonicalize();
    assert!(request.storage_is_canonical());

    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid offset deletion: {error}"));
    assert_eq!(plan.group_id(), "payments");
    assert_eq!(plan.targets()[0].topic(), "orders");
    assert_eq!(plan.targets()[1].topic(), "audit");
}

#[test]
fn invalid_partition_remains_inert_until_plan_conversion() {
    let request = DeleteConsumerGroupOffsetsRequest::new(
        "payments".to_owned(),
        vec![DeleteConsumerGroupOffsetTarget::new(
            "orders".to_owned(),
            i32::MIN,
        )],
    );
    assert!(request.into_plan().is_err());
}

fn oversized(value: &str) -> String {
    let mut owned = String::with_capacity(128);
    owned.push_str(value);
    owned
}
