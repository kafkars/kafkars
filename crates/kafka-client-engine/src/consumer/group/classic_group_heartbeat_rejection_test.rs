//! Atomic Heartbeat rejection installation and Rediscover deferral scenarios.

use kafka_client_core::{
    ClassicBrokerError, ClassicCoordinatorRecovery, ClassicGroupEffect, ClassicGroupInput, GroupId,
    Moment,
};

use super::{
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_heartbeat_rejection::install_heartbeat_rejection,
    classic_group_rejection_fault::ClassicRejectionInstallFailure, classic_group_test_support,
    registry_entry::GroupConsumerEntry,
};

#[test]
fn rediscovery_retains_both_effects_without_partially_revoking_the_catalog() {
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

    let fault = install_heartbeat_rejection(&mut entry, transition)
        .err()
        .unwrap_or_else(|| panic!("rediscovery must remain deferred"));

    assert_eq!(
        fault.failure(),
        ClassicRejectionInstallFailure::CoordinatorRediscovery
    );
    assert!(matches!(
        &fault.effects()[0],
        Some(ClassicGroupEffect::Revoke { .. })
    ));
    assert!(matches!(
        &fault.effects()[1],
        Some(ClassicGroupEffect::ArmRejoin {
            coordinator: ClassicCoordinatorRecovery::Rediscover,
            ..
        })
    ));
    assert!(entry.catalog.live_assignment().is_some());
    assert!(entry.rejoin.is_dormant());
}

fn stable_entry() -> GroupConsumerEntry {
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
