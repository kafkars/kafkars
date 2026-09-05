//! Ordinary Heartbeat-loss atomic recovery installation evidence.

use kafka_client_core::{
    ClassicBrokerError, ClassicCoordinatorRecovery, ClassicGroupEffect, ClassicGroupInput, Moment,
};

use crate::consumer::GroupConsumerEvent;

use super::{
    super::{
        classic_group_assignment::retire_and_revoke_classic_group_assignment,
        classic_group_graceful_revocation::{
            ClassicGroupRevocationAcknowledgeError, ClassicGroupRevocationTurn,
        },
        classic_group_heartbeat::ClassicHeartbeatExecutionState,
        classic_group_heartbeat_rejection::install_heartbeat_rejection,
        classic_group_heartbeat_rejection_test::stable_entry,
        registry_entry::GroupConsumerEntry,
        registry_graceful_revocation::GroupConsumerRevocationPortError,
        registry_test_support::{
            install_ready_group_delivery, install_session, register, started_registry,
            stop_registry,
        },
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

#[test]
fn rebalance_after_an_unfetched_assignment_keeps_the_public_revocation_epoch() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    retire_before_fetch(&mut registry.entries[0]);
    install_session(&mut registry, group_id);
    install_ready_group_delivery(&mut registry, group_id, 17);

    let entry = &mut registry.entries[0];
    entry.catalog.stage_installed_assignment_event();
    entry.catalog.confirm_sync_event();
    let membership_epoch = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("replacement membership"))
        .assignment_generation()
        .get();
    let fetch_epoch = entry
        .fetch
        .machine_assignment_epoch()
        .unwrap_or_else(|| panic!("first Fetch activation"))
        .get();
    assert_eq!((membership_epoch, fetch_epoch), (2, 1));
    let ClassicHeartbeatExecutionState::Waiting(heartbeat) = entry.heartbeat.state() else {
        panic!("replacement heartbeat schedule");
    };
    let attempt = heartbeat.attempt();
    let now = Moment::from_tick(heartbeat.due().tick());
    entry
        .classic
        .apply(ClassicGroupInput::HeartbeatDue { attempt, now })
        .unwrap_or_else(|error| panic!("heartbeat due: {error}"));
    let transition = entry
        .classic
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt,
            now,
            error: ClassicBrokerError::try_from_code(27)
                .unwrap_or_else(|| panic!("rebalance-in-progress code")),
        })
        .unwrap_or_else(|error| panic!("broker rebalance: {error}"));
    install_heartbeat_rejection(entry, transition, now)
        .unwrap_or_else(|fault| panic!("rebalance install: {:?}", fault.failure()));
    entry
        .heartbeat
        .clear_local()
        .unwrap_or_else(|error| panic!("local heartbeat retirement: {error:?}"));
    assert!(entry.fault.is_none());

    let event = registry
        .take_event(group_id)
        .unwrap_or_else(|error| panic!("revocation event: {error:?}"));
    let Some(GroupConsumerEvent::PartitionsRevoked(assignment)) = event else {
        panic!("public revocation event");
    };
    assert_eq!(assignment.assignment_epoch(), membership_epoch);
    assert_eq!(
        registry.acknowledge_revocation(group_id, fetch_epoch, now),
        Err(GroupConsumerRevocationPortError::Acknowledge(
            ClassicGroupRevocationAcknowledgeError::AssignmentEpochMismatch,
        ))
    );
    registry
        .acknowledge_revocation(group_id, membership_epoch, now)
        .unwrap_or_else(|error| panic!("public epoch acknowledgement: {error:?}"));
    assert_eq!(
        registry.turn_graceful_revocation(now),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    let entry = &registry.entries[0];
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.fetch.activation().is_none());
    assert!(entry.revocation.is_dormant());
    assert!(entry.fault.is_none());
    stop_registry(&mut registry);
}

fn retire_before_fetch(entry: &mut GroupConsumerEntry) {
    assert!(entry.fetch.activation().is_none());
    let cycle = entry
        .classic
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("first membership cycle"));
    let transition = entry
        .classic
        .apply(ClassicGroupInput::AssignmentLost { cycle })
        .unwrap_or_else(|error| panic!("early assignment loss: {error}"));
    let Some(ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    }) = transition.into_effects().next()
    else {
        panic!("early assignment retirement");
    };
    retire_and_revoke_classic_group_assignment(
        &entry.classic,
        &mut entry.catalog,
        &mut entry.processing_lease,
        &mut entry.fetch,
        assignment,
        classic_generation,
    )
    .unwrap_or_else(|failure| panic!("early retirement: {:?}", failure.kind));
    entry
        .heartbeat
        .clear_local()
        .unwrap_or_else(|error| panic!("early heartbeat retirement: {error:?}"));
}
