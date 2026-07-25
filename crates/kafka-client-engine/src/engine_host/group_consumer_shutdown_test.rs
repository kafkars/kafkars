//! Group notifier handoff after terminal shard fencing.

use std::sync::Arc;

use crate::consumer::{
    GroupConsumerRegistry, GroupConsumerShardOwner, GroupConsumerShardWake,
    GroupConsumerShardWakeError,
};

use super::group_consumer_shutdown::stop;

struct NoopWake;

impl GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        Ok(())
    }
}

#[test]
fn terminal_shard_stop_returns_the_exact_notifier_owner() {
    let registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("group registry: {error}"));
    let (owner, _port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::new(NoopWake),
    );

    let (stopped, fallback) = stop(&owner);

    assert!(fallback.is_none());
    let notifier = stopped.unwrap_or_else(|error| panic!("group stop failed: {error}"));
    notifier
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
