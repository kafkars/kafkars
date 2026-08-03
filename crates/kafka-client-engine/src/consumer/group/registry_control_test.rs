//! Registry validation, one-wake coalescing, and current-assignment control scenarios.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::consumer::group_control::GroupConsumerPartition;

use super::{
    registry_control::GroupConsumerControlPortError,
    registry_entry::GroupConsumerEntryState,
    registry_shard::GroupConsumerShardOwner,
    registry_test_support::{
        install_ready_group_delivery, install_session, register, started_registry, stop_registry,
    },
    registry_wake::{GroupConsumerShardWake, GroupConsumerShardWakeError},
};

#[test]
fn duplicate_and_unknown_targets_reject_before_pause_mutation() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    install_ready_group_delivery(&mut registry, group_id, 17);
    let orders = target("orders", 0);

    assert_eq!(
        registry.pause_partitions(group_id, &[target("orders", 0), target("orders", 0)]),
        Err(GroupConsumerControlPortError::DuplicatePartition)
    );
    assert_eq!(
        registry.pause_partitions(group_id, &[target("unknown", 0)]),
        Err(GroupConsumerControlPortError::UnknownPartition)
    );
    let paused = registry
        .pause_partitions(group_id, &[orders])
        .unwrap_or_else(|error| panic!("pause after rejections: {error:?}"));
    assert_eq!(paused.effects(), 1);
    stop_registry(&mut registry);
}

#[test]
fn absent_assignment_and_closing_entry_are_stable_unavailable_states() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    assert_eq!(
        registry.pause_partitions(group_id, &[target("orders", 0)]),
        Err(GroupConsumerControlPortError::NoAssignment)
    );
    install_session(&mut registry, group_id);
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    entry.state = GroupConsumerEntryState::Closing;
    assert_eq!(
        registry.pause_partitions(group_id, &[target("orders", 0)]),
        Err(GroupConsumerControlPortError::GroupUnavailable)
    );
    stop_registry(&mut registry);
}

#[test]
fn accepted_batch_requests_exactly_one_turn_after_releasing_the_registry() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    install_ready_group_delivery(&mut registry, group_id, 17);
    let wake = Arc::new(CountingWake::default());
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::clone(&wake),
    );

    let accepted = port
        .try_pause_partitions(group_id, &[target("orders", 0)])
        .unwrap_or_else(|error| panic!("port pause: {error:?}"));
    assert!(!accepted.retained_invariant());
    assert!(!accepted.wake_failed());
    assert_eq!(wake.requests.load(Ordering::Acquire), 1);

    let mut registry = owner.terminal_registry();
    registry
        .recover_fetch_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover group Fetch: {error:?}"));
    stop_registry(&mut registry);
}

#[derive(Default)]
struct CountingWake {
    requests: AtomicUsize,
}

impl GroupConsumerShardWake for CountingWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        self.requests.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

fn target(topic: &str, partition: i32) -> GroupConsumerPartition {
    GroupConsumerPartition::try_new(topic, partition)
        .unwrap_or_else(|error| panic!("control target: {error}"))
}
