//! Ordinary Heartbeat-loss atomic recovery installation evidence.

use kafka_client_core::{
    ClassicCoordinatorRecovery, ClassicGroupEffect, ClassicGroupInput, Moment,
};

use super::{
    super::{
        classic_group_heartbeat::ClassicHeartbeatExecutionState,
        classic_group_heartbeat_rejection_test::stable_entry,
    },
    recovery::install_recovery,
};

#[test]
fn retained_route_recovery_revokes_before_installing_the_bounded_rejoin() {
    let mut entry = stable_entry();
    let ClassicHeartbeatExecutionState::Waiting(heartbeat) = entry.heartbeat.state() else {
        panic!("heartbeat schedule expected");
    };
    let heartbeat = *heartbeat;
    let attempt = heartbeat.attempt();
    let now = Moment::from_tick(heartbeat.due().tick());
    entry
        .classic
        .apply(ClassicGroupInput::HeartbeatDue { attempt, now })
        .unwrap_or_else(|error| panic!("heartbeat due failed: {error}"));
    let transition = entry
        .classic
        .apply(ClassicGroupInput::HeartbeatFailed { attempt, now })
        .unwrap_or_else(|error| panic!("heartbeat loss failed: {error}"));
    let mut effects = transition.into_effects();
    let Some(ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    }) = effects.next()
    else {
        panic!("assignment revoke expected first");
    };
    let Some(ClassicGroupEffect::ArmRejoin {
        schedule,
        coordinator: ClassicCoordinatorRecovery::Retain,
    }) = effects.next()
    else {
        panic!("retained rejoin expected second");
    };
    assert!(effects.next().is_none());

    install_recovery(
        &mut entry,
        assignment,
        classic_generation,
        schedule,
        now,
        ClassicCoordinatorRecovery::Retain,
    )
    .unwrap_or_else(|fault| panic!("recovery install failed: {:?}", fault.failure()));

    assert!(entry.catalog.live_assignment().is_some());
    assert!(!entry.revocation.is_dormant());
    assert_eq!(entry.rejoin.schedule(), Some(schedule));
    assert!(!entry.rediscovery.blocks_join());
}
