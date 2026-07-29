//! Partition-transaction abort plan validation scenarios.

use super::{
    ABORT_PARTITION_TRANSACTION_MAX_TOPIC_NAME_BYTES, AbortPartitionTransactionPlan,
    AbortPartitionTransactionPlanError,
};

#[test]
fn plan_preserves_the_complete_nonnegative_transaction_identity() {
    let plan = AbortPartitionTransactionPlan::new("orders".to_owned(), 2, 91, 7, 11)
        .unwrap_or_else(|error| panic!("plan: {error}"));

    assert_eq!(plan.topic(), "orders");
    assert_eq!(plan.partition(), 2);
    assert_eq!(plan.producer_id(), 91);
    assert_eq!(plan.producer_epoch(), 7);
    assert_eq!(plan.coordinator_epoch(), 11);
    assert_eq!(plan.transaction_version(), 0);
    assert_eq!(plan.minimum_api_version(), 1);
    assert_eq!(plan.into_parts(), ("orders".to_owned(), 2, 91, 7, 11, 0));
}

#[test]
fn every_positive_transaction_version_requires_api_v2() {
    for transaction_version in [1, 2, i8::MAX] {
        let plan = AbortPartitionTransactionPlan::new("orders".to_owned(), 2, 91, 7, 11)
            .unwrap_or_else(|error| panic!("plan: {error}"))
            .with_transaction_version(transaction_version)
            .unwrap_or_else(|error| panic!("transaction version: {error}"));

        assert_eq!(plan.transaction_version(), transaction_version);
        assert_eq!(plan.minimum_api_version(), 2);
        assert_eq!(plan.into_parts().5, transaction_version);
    }
}

#[test]
fn explicit_zero_transaction_version_retains_v1_compatibility() {
    let plan = AbortPartitionTransactionPlan::new("orders".to_owned(), 2, 91, 7, 11)
        .unwrap_or_else(|error| panic!("plan: {error}"))
        .with_transaction_version(0)
        .unwrap_or_else(|error| panic!("transaction version: {error}"));

    assert_eq!(plan.transaction_version(), 0);
    assert_eq!(plan.minimum_api_version(), 1);
}

#[test]
fn negative_transaction_versions_are_rejected_before_machine_construction() {
    for transaction_version in [i8::MIN, -1] {
        let plan = AbortPartitionTransactionPlan::new("orders".to_owned(), 2, 91, 7, 11)
            .unwrap_or_else(|error| panic!("plan: {error}"));
        assert_eq!(
            plan.with_transaction_version(transaction_version),
            Err(AbortPartitionTransactionPlanError::NegativeTransactionVersion)
        );
    }
}

#[test]
fn plan_rejects_every_invalid_identity_before_machine_construction() {
    for (plan, expected) in [
        (
            AbortPartitionTransactionPlan::new(String::new(), 0, 1, 1, 1),
            AbortPartitionTransactionPlanError::EmptyTopicName,
        ),
        (
            AbortPartitionTransactionPlan::new(
                "x".repeat(ABORT_PARTITION_TRANSACTION_MAX_TOPIC_NAME_BYTES + 1),
                0,
                1,
                1,
                1,
            ),
            AbortPartitionTransactionPlanError::TopicNameTooLong,
        ),
        (
            AbortPartitionTransactionPlan::new("orders".to_owned(), -1, 1, 1, 1),
            AbortPartitionTransactionPlanError::NegativePartition,
        ),
        (
            AbortPartitionTransactionPlan::new("orders".to_owned(), 0, -1, 1, 1),
            AbortPartitionTransactionPlanError::NegativeProducerId,
        ),
        (
            AbortPartitionTransactionPlan::new("orders".to_owned(), 0, 1, -1, 1),
            AbortPartitionTransactionPlanError::NegativeProducerEpoch,
        ),
        (
            AbortPartitionTransactionPlan::new("orders".to_owned(), 0, 1, 1, -1),
            AbortPartitionTransactionPlanError::NegativeCoordinatorEpoch,
        ),
    ] {
        assert_eq!(plan, Err(expected));
    }
}

#[test]
fn maximum_topic_and_scalar_values_remain_representable() {
    let topic = "x".repeat(ABORT_PARTITION_TRANSACTION_MAX_TOPIC_NAME_BYTES);
    let plan =
        AbortPartitionTransactionPlan::new(topic.clone(), i32::MAX, i64::MAX, i16::MAX, i32::MAX)
            .unwrap_or_else(|error| panic!("maximum values: {error}"));

    assert_eq!(plan.topic(), topic);
    assert_eq!(plan.partition(), i32::MAX);
    assert_eq!(plan.producer_id(), i64::MAX);
    assert_eq!(plan.producer_epoch(), i16::MAX);
    assert_eq!(plan.coordinator_epoch(), i32::MAX);
    assert_eq!(plan.transaction_version(), 0);
}
