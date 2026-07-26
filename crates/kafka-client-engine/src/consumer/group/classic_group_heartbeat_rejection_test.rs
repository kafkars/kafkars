//! Atomic Heartbeat rejection revocation and Rediscover installation scenarios.

use kafka_client_core::{ClassicBrokerError, ClassicGroupInput, GroupId, Moment};

use super::{
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_heartbeat_rejection::install_heartbeat_rejection, classic_group_test_support,
    registry_entry::GroupConsumerEntry,
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
    let group_id =
        GroupId::try_from_raw(78).unwrap_or_else(|| panic!("nonzero group identity expected"));
    let mut entry = GroupConsumerEntry::try_new(
        group_id,
        &std::sync::Arc::from("workers"),
        &[std::sync::Arc::from("orders")],
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    )
    .unwrap_or_else(|error| panic!("entry creation failed: {error:?}"));
    let heartbeat = classic_group_test_support::install_follower(
        &mut entry.catalog,
        &mut entry.classic,
        "member-a",
        7,
        Vec::new(),
    );
    entry
        .heartbeat
        .prepare_install(heartbeat)
        .unwrap_or_else(|error| panic!("heartbeat install failed: {error:?}"))
        .commit();
    entry
}
