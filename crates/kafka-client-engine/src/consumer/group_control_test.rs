//! Stable scalar validation and exact rejected-batch recovery scenarios.

use std::sync::Arc;

use super::{
    GroupConsumerControlErrorKind, GroupConsumerHandle, GroupConsumerPartition,
    GroupConsumerPartitionInputErrorKind, GroupConsumerRegistration, GroupConsumerShardOwner,
    group::{
        install_group_session_for_public_test, install_ready_group_delivery_for_public_test,
        started_group_registry_for_public_test,
    },
};

#[test]
fn scalar_targets_validate_topic_and_partition_bounds() {
    for (topic, partition, kind) in [
        ("", 0, GroupConsumerPartitionInputErrorKind::EmptyTopic),
        (
            "orders",
            -1,
            GroupConsumerPartitionInputErrorKind::NegativePartition,
        ),
    ] {
        let error = GroupConsumerPartition::try_new(topic, partition)
            .err()
            .unwrap_or_else(|| panic!("invalid target"));
        assert_eq!(error.kind(), kind);
    }
    let long = "x".repeat(250);
    assert_eq!(
        GroupConsumerPartition::try_new(long, 0)
            .err()
            .unwrap_or_else(|| panic!("long topic"))
            .kind(),
        GroupConsumerPartitionInputErrorKind::TopicTooLong
    );
}

#[test]
fn contended_control_returns_the_exact_vector_for_retry() {
    let registry = started_group_registry_for_public_test();
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::new(NoopWake),
    );
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let mut handle = GroupConsumerHandle::try_register(
        port,
        Arc::clone(&lifetime),
        GroupConsumerRegistration::new(Arc::from("workers"), vec![Arc::from("orders")]),
    )
    .unwrap_or_else(|error| panic!("registration: {error}"));
    let group_id = handle.group_id_for_test();
    {
        let mut registry = owner.lock_registry_for_test();
        install_group_session_for_public_test(&mut registry, group_id);
        install_ready_group_delivery_for_public_test(&mut registry, group_id, 17);
    }
    let lock = owner.lock_registry_for_test();

    let error = handle
        .pause(vec![target("orders", 0)])
        .err()
        .unwrap_or_else(|| panic!("contended pause"));
    assert_eq!(error.kind(), GroupConsumerControlErrorKind::Contended);
    assert_eq!(error.partitions()[0].topic(), "orders");
    let rejected = error.into_partitions();
    assert_eq!(rejected[0].partition(), 0);

    drop(lock);
    let _accepted = handle
        .pause(rejected)
        .unwrap_or_else(|error| panic!("retry pause: {error}"));
    drop(handle);
    let mut registry = owner.terminal_registry();
    registry.recover_fetch_after_driver_shutdown();
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish registry: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

#[test]
fn empty_batch_is_inert_even_while_the_registry_is_contended() {
    let registry = started_group_registry_for_public_test();
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::new(NoopWake),
    );
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let group_id =
        kafka_client_core::GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group identity"));
    let mut handle = GroupConsumerHandle::from_registered_for_test(port, lifetime, group_id);
    let lock = owner.lock_registry_for_test();

    assert_eq!(
        handle
            .pause(Vec::new())
            .unwrap_or_else(|error| panic!("empty pause: {error}"))
            .fault(),
        None
    );

    drop(lock);
    drop(handle);
    let mut registry = owner.terminal_registry();
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish registry: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

fn target(topic: &str, partition: i32) -> GroupConsumerPartition {
    GroupConsumerPartition::try_new(topic, partition)
        .unwrap_or_else(|error| panic!("control target: {error}"))
}

struct NoopWake;

impl super::GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), super::GroupConsumerShardWakeError> {
        Ok(())
    }
}
