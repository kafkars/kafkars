//! Inert and canonical offset-alteration request scenarios.

use super::{AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsRequest};

#[test]
fn request_canonicalizes_owned_storage_and_preserves_caller_order() {
    let request = AlterConsumerGroupOffsetsRequest::new(
        oversized("payments"),
        vec![
            AlterConsumerGroupOffsetTarget::new(
                oversized("orders"),
                2,
                91,
                Some(7),
                Some(oversized("checkpoint-a")),
            ),
            AlterConsumerGroupOffsetTarget::new(oversized("audit"), 0, 42, None, None),
        ],
    )
    .canonicalize();
    assert!(request.storage_is_canonical());

    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid offset alteration: {error}"));
    assert_eq!(plan.group_id(), "payments");
    assert_eq!(plan.targets()[0].topic(), "orders");
    assert_eq!(plan.targets()[1].topic(), "audit");
}

#[test]
fn invalid_partition_remains_inert_until_plan_conversion() {
    let request = AlterConsumerGroupOffsetsRequest::new(
        "payments".to_owned(),
        vec![AlterConsumerGroupOffsetTarget::new(
            "orders".to_owned(),
            i32::MIN,
            1,
            None,
            None,
        )],
    );
    assert!(request.into_plan().is_err());
}

fn oversized(value: &str) -> String {
    let mut owned = String::with_capacity(128);
    owned.push_str(value);
    owned
}
