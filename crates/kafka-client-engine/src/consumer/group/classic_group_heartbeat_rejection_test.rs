//! Atomic Heartbeat rejection retained-rejoin, revocation, and Rediscover scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ClassicBrokerError, ClassicGroupInput, ClassicGroupPhase, GroupPositionMissingOffsetPolicy,
    Moment, ReadIsolation,
};

use crate::{
    config::{ValidatedConsumerFetchConfig, ValidatedConsumerLimits},
    consumer::{
        GroupConsumerEvent,
        group_registration_request::{GroupConsumerClassicAssignor, GroupConsumerProtocol},
    },
};

use super::{
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_heartbeat_rejection::install_heartbeat_rejection,
    classic_group_test_support,
    registry_entry::{GroupConsumerEntry, default_classic_processing_lease_policy},
    registry_test_support::{
        install_ready_group_delivery, install_session, register, started_registry,
    },
};

#[test]
fn cooperative_rebalance_retains_live_ownership_and_installs_only_rejoin() {
    let mut entry = cooperative_stable_entry();
    assert!(matches!(
        entry.catalog.take_event(),
        Some(GroupConsumerEvent::PartitionsAssigned(_))
    ));
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment expected"));
    let group_id = assignment.group_id();
    let member_id = assignment.member_id();
    let assignment_generation = assignment.assignment_generation();
    let partitions = assignment.partitions().to_vec();
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
            error: ClassicBrokerError::try_from_code(27)
                .unwrap_or_else(|| panic!("nonzero broker error")),
        })
        .unwrap_or_else(|error| panic!("heartbeat rejection failed: {error}"));

    install_heartbeat_rejection(
        &mut entry,
        transition,
        Moment::from_tick(schedule.due().tick()),
    )
    .unwrap_or_else(|_fault| panic!("cooperative retained-rejoin installation failed"));

    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(
        entry.rejoin.schedule(),
        entry.classic.machine().pending_rejoin()
    );
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment must remain installed"));
    assert_eq!(assignment.group_id(), group_id);
    assert_eq!(assignment.member_id(), member_id);
    assert_eq!(assignment.assignment_generation(), assignment_generation);
    assert_eq!(assignment.partitions(), partitions);
    assert_eq!(entry.catalog.classic_generation(), Some(7));
    assert!(entry.fetch.activation().is_some());
    assert!(entry.processing_lease.active_schedule().is_some());
    assert!(entry.revocation.is_dormant());
    assert!(entry.catalog.take_event().is_none());
    assert!(!entry.rediscovery.blocks_join());
}

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

    install_heartbeat_rejection(
        &mut entry,
        transition,
        Moment::from_tick(schedule.due().tick()),
    )
    .unwrap_or_else(|fault| panic!("rediscovery installation failed: {:?}", fault.failure()));

    assert!(entry.catalog.live_assignment().is_some());
    assert!(!entry.revocation.is_dormant());
    assert!(matches!(
        entry.catalog.take_event(),
        Some(GroupConsumerEvent::PartitionsRevoked(_))
    ));
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
    install_ready_group_delivery(&mut registry, group_id, 17);
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    entry.catalog.stage_installed_assignment_event();
    entry.catalog.confirm_sync_event();
    registry.entries.remove(0)
}

fn cooperative_stable_entry() -> GroupConsumerEntry {
    let mut registry = started_registry();
    let group_id = registry
        .try_register_with_protocol_configuration(
            Arc::from("workers"),
            None,
            vec![Arc::from("orders")],
            GroupConsumerProtocol::Classic,
            GroupConsumerClassicAssignor::CooperativeSticky,
            classic_group_test_support::timing(),
            classic_group_test_support::heartbeat_policy(),
            classic_group_test_support::rejoin_policy(),
            GroupPositionMissingOffsetPolicy::Error,
            ReadIsolation::ReadUncommitted,
            default_classic_processing_lease_policy(),
            ValidatedConsumerFetchConfig::default(),
            ValidatedConsumerLimits::default(),
        )
        .unwrap_or_else(|failure| panic!("cooperative registration: {:?}", failure.kind));
    install_session(&mut registry, group_id);
    install_ready_group_delivery(&mut registry, group_id, 17);
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered cooperative entry expected"));
    entry.catalog.stage_installed_assignment_event();
    entry.catalog.confirm_sync_event();
    registry.entries.remove(0)
}
