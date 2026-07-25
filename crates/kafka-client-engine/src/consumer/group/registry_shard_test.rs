//! Unique group-registry shard ownership and terminal fencing scenarios.

use std::sync::Arc;

use super::{
    classic_group_test_support,
    registry::GroupConsumerRegistry,
    registry_shard::GroupConsumerShardOwner,
    registry_wake::{GroupConsumerShardWake, GroupConsumerShardWakeError},
};

struct NoopWake;

impl GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        Ok(())
    }
}

#[test]
fn terminal_registry_closes_admission_under_the_owner_lock() {
    let registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::new(NoopWake),
    );

    let mut registry = owner.terminal_registry();
    let failure = registry
        .try_register(
            Arc::from("workers"),
            vec![Arc::from("orders")],
            classic_group_test_support::timing(),
            classic_group_test_support::heartbeat_policy(),
            classic_group_test_support::rejoin_policy(),
        )
        .err()
        .unwrap_or_else(|| panic!("terminal registry must reject admission"));
    assert_eq!(
        failure.kind,
        super::registry::GroupConsumerRegistrationFailureKind::Closed
    );
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry stop failed: {error}"));
    drop(registry);
    drop(port);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join failed: {error}"));
}
