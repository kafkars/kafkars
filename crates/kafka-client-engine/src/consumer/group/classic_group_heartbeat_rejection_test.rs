//! Atomic Heartbeat rejection revocation and Rediscover installation scenarios.

use kafka_client_core::{ClassicBrokerError, ClassicGroupInput, Moment};

use super::{
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_heartbeat_rejection::install_heartbeat_rejection,
    registry_entry::GroupConsumerEntry,
    registry_test_support::{install_session, register, started_registry},
};

#[test]
fn rediscovery_revokes_then_blocks_join_until_route_transfer() {
    let mut entry = stable_entry();
    let ClassicHeartbeatExecutionState::Waiting(schedule) = entry.heartbeat.state() else {
        panic!("heartbeat schedule expected");
    };
    let schedule = *schedule;
    let attempt = schedule.attempt();
    entry
        .classic
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt,
            now: Moment::from_tick(schedule.due().tick()),
        })
        .unwrap_or_else(|error| panic!("heartbeat due failed: {error}"));
    let transition = entry
        .classic
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt,
            now: Moment::from_tick(schedule.due().tick()),
            error: ClassicBrokerError::try_from_code(15)
                .unwrap_or_else(|| panic!("nonzero broker error")),
        })
        .unwrap_or_else(|error| panic!("heartbeat rejection failed: {error}"));

    install_heartbeat_rejection(&mut entry, transition)
        .unwrap_or_else(|_fault| panic!("rediscovery installation failed"));

    assert!(entry.catalog.live_assignment().is_none());
    assert_eq!(
        entry.rejoin.schedule(),
        entry.classic.machine().pending_rejoin()
    );
    assert!(entry.rediscovery.blocks_join());
    assert!(entry.rediscovery.awaits_route_transfer());
}

pub(super) fn stable_entry() -> GroupConsumerEntry {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    registry.entries.remove(0)
}
