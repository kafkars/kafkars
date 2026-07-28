//! Atomic Heartbeat rejection revocation and Rediscover mechanism installation.

use kafka_client_core::{
    ClassicBrokerError, ClassicCoordinatorRecovery, ClassicGroupEffect, ClassicGroupInput, Moment,
};

use super::{
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_heartbeat_rejection_install::install_rediscovery,
    classic_group_heartbeat_rejection_test::stable_entry,
};

#[test]
fn rediscovery_install_commits_revoke_rejoin_and_gate_together() {
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
    let mut effects = transition.into_effects();
    let Some(ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    }) = effects.next()
    else {
        panic!("assignment revoke expected");
    };
    let Some(ClassicGroupEffect::ArmRejoin {
        schedule,
        coordinator: ClassicCoordinatorRecovery::Rediscover,
    }) = effects.next()
    else {
        panic!("Rediscover rejoin expected");
    };
    assert!(effects.next().is_none());

    install_rediscovery(
        &mut entry,
        assignment,
        classic_generation,
        schedule,
        Moment::from_tick(schedule.due().tick()),
    )
    .unwrap_or_else(|fault| panic!("rediscovery installation failed: {:?}", fault.failure()));

    assert!(entry.catalog.live_assignment().is_some());
    assert!(!entry.revocation.is_dormant());
    assert_eq!(
        entry.rejoin.schedule(),
        entry.classic.machine().pending_rejoin()
    );
    assert!(entry.rediscovery.blocks_join());
    assert!(entry.rediscovery.awaits_route_transfer());
}
