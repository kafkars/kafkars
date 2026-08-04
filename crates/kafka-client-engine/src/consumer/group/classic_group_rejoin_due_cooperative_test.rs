//! Retained cooperative ownership gates and exact Join member evidence.

use std::sync::Arc;

use kafka_client_core::{
    ClassicBrokerError, ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase,
    ClassicRejoinSchedule, GroupId, GroupPositionMissingOffsetPolicy, MemberId, Moment,
    PartitionIndex, ReadIsolation,
};

use crate::consumer::group_registration_request::{
    GroupConsumerClassicAssignor, GroupConsumerProtocol,
};
use crate::{
    clock::MonotonicClock,
    config::{ValidatedConsumerFetchConfig, ValidatedConsumerLimits},
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_heartbeat_rejection::install_heartbeat_rejection,
    classic_group_rejoin_due::{
        ClassicGroupRejoinDueTurn, apply_due_transition, expected_due_join_member, stage_due_join,
    },
    classic_group_rejoin_fault::ClassicRejoinPostCoreFailure,
    classic_group_test_support,
    registry_entry::{GroupConsumerEntry, default_classic_processing_lease_policy},
    registry_event_reconciliation_test::prepared_reconciliation,
    registry_test_support::{register, started_registry, stop_registry},
};

#[test]
fn retained_cooperative_assignment_stages_the_exact_member_join() {
    let (mut entry, schedule, member_id) = cooperative_waiting_entry();
    let expected = expected_due_join_member(&entry, schedule)
        .unwrap_or_else(|error| panic!("exact retained due state: {error:?}"));
    assert_eq!(expected, Some(member_id));

    let pending = apply_due_transition(
        &mut entry,
        schedule,
        Moment::from_tick(schedule.due().tick()),
        expected,
    )
    .unwrap_or_else(|error| panic!("cooperative due transition: {error:?}"))
    .unwrap_or_else(|| panic!("retained cooperative Join"));

    let next_cycle = schedule
        .cycle()
        .checked_next()
        .unwrap_or_else(|| panic!("next cooperative cycle"));
    stage_due_join(
        &mut entry,
        schedule,
        schedule.cycle(),
        pending,
        &MonotonicClock::new(),
    )
    .unwrap_or_else(|error| panic!("stage retained cooperative Join: {error:?}"));
    let prepared = entry
        .execution
        .prepared_join()
        .unwrap_or_else(|| panic!("staged retained cooperative Join"));
    assert_eq!(prepared.member_id(), Some(member_id));
    assert_eq!(
        prepared.protocol(),
        kafka_client_core::ClassicProtocol::CooperativeSticky
    );
    assert_eq!(prepared.cycle(), next_cycle);
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Joining);
    assert!(entry.rejoin.is_dormant());
    assert!(entry.fault.is_none());
}

#[test]
fn catalog_and_core_assignment_disagreement_rejects_before_core_mutation() {
    let (mut entry, schedule, _member_id) = cooperative_waiting_entry();
    entry.catalog.current = None;

    assert_eq!(
        expected_due_join_member(&entry, schedule),
        Err(ClassicGroupExecutionError::RejoinState)
    );
    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(entry.classic.machine().pending_rejoin(), Some(schedule));
    assert!(entry.fault.is_none());
}

#[test]
fn mismatched_emitted_member_is_retained_as_a_post_core_shape_fault() {
    let (mut entry, schedule, _member_id) = cooperative_waiting_entry();

    assert_eq!(
        apply_due_transition(
            &mut entry,
            schedule,
            Moment::from_tick(schedule.due().tick()),
            None,
        )
        .err(),
        Some(ClassicGroupExecutionError::RejoinPostCore)
    );
    let Some(ClassicGroupEntryFault::RejoinPostCore(fault)) = entry.fault.as_ref() else {
        panic!("post-core retained member mismatch");
    };
    assert_eq!(fault.failure(), ClassicRejoinPostCoreFailure::EffectShape);
    assert!(fault.join().is_none());
    assert!(matches!(
        fault.other(),
        [
            Some(ClassicGroupEffect::Join {
                member_id: Some(_),
                ..
            }),
            None
        ]
    ));
}

#[test]
fn due_rejoin_waits_for_the_application_owned_reconciliation() {
    let mut pending = prepared_reconciliation();
    let heartbeat = pending
        .classic_reconciliation
        .as_ref()
        .unwrap_or_else(|| panic!("pending cooperative reconciliation"))
        .reconciliation()
        .heartbeat();
    pending
        .heartbeat
        .prepare_install(heartbeat)
        .unwrap_or_else(|error| panic!("reconciliation Heartbeat install: {error:?}"))
        .commit();
    pending
        .classic
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt: heartbeat.attempt(),
            now: Moment::from_tick(heartbeat.due().tick()),
        })
        .unwrap_or_else(|error| panic!("reconciliation Heartbeat due: {error}"));
    let transition = pending
        .classic
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt: heartbeat.attempt(),
            now: Moment::from_tick(heartbeat.due().tick()),
            error: ClassicBrokerError::try_from_code(27)
                .unwrap_or_else(|| panic!("rebalance-in-progress")),
        })
        .unwrap_or_else(|error| panic!("reconciliation Heartbeat rejection: {error}"));
    install_heartbeat_rejection(
        &mut pending,
        transition,
        Moment::from_tick(heartbeat.due().tick()),
    )
    .unwrap_or_else(|_fault| panic!("install deferred reconciliation rejoin"));
    pending
        .heartbeat
        .clear_local()
        .unwrap_or_else(|error| panic!("confirm rejected Heartbeat locally: {error:?}"));
    let schedule = pending
        .classic
        .machine()
        .pending_rejoin()
        .unwrap_or_else(|| panic!("deferred rejoin schedule"));

    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let original = core::mem::replace(entry, pending);

    assert_eq!(
        registry.prepare_one_classic_rejoin(
            Moment::from_tick(schedule.due().tick()),
            &MonotonicClock::new(),
        ),
        Ok(ClassicGroupRejoinDueTurn::Idle)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("retained reconciling group"));
    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(entry.classic.machine().pending_rejoin(), Some(schedule));
    assert!(entry.classic_reconciliation.is_some());
    assert!(entry.execution.is_idle());
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("retained reconciling group"));
    let _pending = core::mem::replace(entry, original);
    stop_registry(&mut registry);
}

fn cooperative_waiting_entry() -> (GroupConsumerEntry, ClassicRejoinSchedule, MemberId) {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group identity"));
    let mut entry = GroupConsumerEntry::try_new_with_protocol_configuration(
        group_id,
        &Arc::from("workers"),
        None,
        &[Arc::from("orders")],
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
    .unwrap_or_else(|error| panic!("cooperative entry: {error:?}"));
    let topic_id = entry
        .catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("orders topic identity"));
    let heartbeat = classic_group_test_support::install_follower(
        &mut entry.catalog,
        &mut entry.classic,
        "member-1",
        7,
        vec![kafka_client_core::GroupAssignmentPartition::new(
            topic_id,
            PartitionIndex::from_raw(0),
        )],
    );
    let member_id = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("installed cooperative assignment"))
        .member_id();
    entry
        .classic
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt: heartbeat.attempt(),
            now: Moment::from_tick(heartbeat.due().tick()),
        })
        .unwrap_or_else(|error| panic!("cooperative heartbeat due: {error}"));
    let transition = entry
        .classic
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt: heartbeat.attempt(),
            now: Moment::from_tick(heartbeat.due().tick()),
            error: ClassicBrokerError::try_from_code(27)
                .unwrap_or_else(|| panic!("rebalance-in-progress broker error")),
        })
        .unwrap_or_else(|error| panic!("retain cooperative assignment: {error}"));
    let mut effects = transition.into_effects();
    let Some(ClassicGroupEffect::ArmRejoin { schedule, .. }) = effects.next() else {
        panic!("cooperative retained rejoin schedule");
    };
    assert!(effects.next().is_none());
    entry
        .rejoin
        .prepare_rejoin_install(schedule)
        .unwrap_or_else(|error| panic!("install rejoin schedule: {error:?}"))
        .commit();
    (entry, schedule, member_id)
}
