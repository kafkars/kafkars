//! Inert and canonical offset-alteration request scenarios.

use std::time::Duration;

use kafka_client_core::AlterConsumerGroupOffsetsPlanError;

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
    assert_eq!(plan.retention_time_ms(), None);
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

#[test]
fn explicit_retention_remains_inert_and_preserves_exact_milliseconds() {
    let request = plain_request().with_retention_time(Duration::from_millis(86_400_000));
    let plan = request
        .canonicalize()
        .into_plan()
        .unwrap_or_else(|error| panic!("valid retained offset alteration: {error}"));

    assert_eq!(plan.retention_time_ms(), Some(86_400_000));
}

#[test]
fn oversized_retention_rejects_at_plan_conversion_without_saturation() {
    let request = plain_request().with_retention_time(Duration::from_millis(i64::MAX as u64 + 1));
    assert_eq!(
        request.canonicalize().into_plan(),
        Err(AlterConsumerGroupOffsetsPlanError::RetentionTimeTooLarge)
    );
}

#[test]
fn retention_with_any_leader_epoch_rejects_at_plan_conversion() {
    let request = AlterConsumerGroupOffsetsRequest::new(
        "payments".to_owned(),
        vec![
            AlterConsumerGroupOffsetTarget::new("orders".to_owned(), 0, 1, None, None),
            AlterConsumerGroupOffsetTarget::new("orders".to_owned(), 1, 2, Some(7), None),
        ],
    )
    .with_retention_time(Duration::from_millis(1));
    assert_eq!(
        request.canonicalize().into_plan(),
        Err(AlterConsumerGroupOffsetsPlanError::RetentionTimeWithLeaderEpoch)
    );
}

fn plain_request() -> AlterConsumerGroupOffsetsRequest {
    AlterConsumerGroupOffsetsRequest::new(
        "payments".to_owned(),
        vec![AlterConsumerGroupOffsetTarget::new(
            "orders".to_owned(),
            0,
            1,
            None,
            None,
        )],
    )
}

fn oversized(value: &str) -> String {
    let mut owned = String::with_capacity(128);
    owned.push_str(value);
    owned
}
