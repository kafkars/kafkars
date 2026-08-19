//! Exact cooperative reconciliation fixtures shared by sibling unit tests.

use std::sync::Arc;

use kafka_client_core::{
    ClassicBrokerError, ClassicGeneration, ClassicGroupEffect, ClassicGroupInput,
    ClassicProcessingLeaseFence, ClassicRejoinSchedule, Deadline, GroupAssignmentPartition,
    GroupId, GroupPositionBatch, GroupPositionFence, GroupPositionMissingOffsetPolicy,
    GroupPositionPartitionFact, LiveGroupAssignment, Moment, NextFetchOffset, PartitionIndex,
    ReadIsolation,
};

use crate::{
    clock::OperationDeadline,
    config::{ValidatedConsumerFetchConfig, ValidatedConsumerLimits},
    consumer::group_registration_request::{GroupConsumerClassicAssignor, GroupConsumerProtocol},
};

use super::super::{
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_heartbeat_rejection::install_heartbeat_rejection,
    classic_group_position::{prepare_classic_group_position, test_support::completed_ready},
    classic_group_reconciliation::PreparedClassicGroupReconciliation,
    classic_group_test_support,
    registry_entry::{GroupConsumerEntry, default_classic_processing_lease_policy},
};

#[expect(
    clippy::too_many_lines,
    reason = "the fixture constructs one exact cross-owner reconciliation state for multiple focused assertions"
)]
pub(in super::super) fn prepared_reconciliation() -> GroupConsumerEntry {
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
        vec![
            GroupAssignmentPartition::new(topic_id, PartitionIndex::from_raw(0)),
            GroupAssignmentPartition::new(topic_id, PartitionIndex::from_raw(1)),
        ],
    );
    entry.catalog.stage_installed_assignment_event();
    entry.catalog.confirm_sync_event();
    let _assigned = entry.catalog.take_event();

    let heartbeat_now = Moment::from_tick(heartbeat.due().tick());
    entry
        .classic
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt: heartbeat.attempt(),
            now: heartbeat_now,
        })
        .unwrap_or_else(|error| panic!("cooperative heartbeat due: {error}"));
    let rejection = entry
        .classic
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt: heartbeat.attempt(),
            now: heartbeat_now,
            error: ClassicBrokerError::try_from_code(27)
                .unwrap_or_else(|| panic!("rebalance-in-progress broker error")),
        })
        .unwrap_or_else(|error| panic!("cooperative retained rejection: {error}"));
    let mut effects = rejection.into_effects();
    let Some(ClassicGroupEffect::ArmRejoin { schedule, .. }) = effects.next() else {
        panic!("retained cooperative rejoin schedule");
    };
    assert!(effects.next().is_none());
    let join = entry
        .classic
        .apply(ClassicGroupInput::RejoinDue {
            schedule,
            now: Moment::from_tick(schedule.due().tick()),
        })
        .unwrap_or_else(|error| panic!("retained cooperative rejoin: {error}"));
    let mut effects = join.into_effects();
    let Some(ClassicGroupEffect::Join { cycle, .. }) = effects.next() else {
        panic!("retained cooperative Join");
    };
    assert!(effects.next().is_none());

    let member = Arc::clone(
        entry
            .catalog
            .current_member()
            .unwrap_or_else(|| panic!("retained member spelling")),
    );
    let candidate = entry
        .catalog
        .prepare_follower_cycle(cycle, member)
        .unwrap_or_else(|error| panic!("replacement follower candidate: {error:?}"));
    let member_id = candidate.local_member_id();
    entry
        .classic
        .stage_candidate(candidate)
        .unwrap_or_else(|error| panic!("stage replacement candidate: {error:?}"));
    entry
        .classic
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(schedule.due().tick()),
            member_id,
            generation: ClassicGeneration::try_from_raw(8)
                .unwrap_or_else(|| panic!("replacement generation")),
        })
        .unwrap_or_else(|error| panic!("replacement follower Join: {error}"));
    let sync = entry
        .classic
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(schedule.due().tick()),
            partitions: vec![
                GroupAssignmentPartition::new(topic_id, PartitionIndex::from_raw(0)),
                GroupAssignmentPartition::new(topic_id, PartitionIndex::from_raw(2)),
            ],
        })
        .unwrap_or_else(|error| panic!("replacement cooperative Sync: {error}"));
    let mut effects = sync.into_effects();
    let Some(ClassicGroupEffect::Reconcile { reconciliation }) = effects.next() else {
        panic!("cooperative reconciliation effect");
    };
    assert!(effects.next().is_none());

    let candidate = entry
        .classic
        .pending()
        .unwrap_or_else(|| panic!("replacement candidate retained"));
    entry
        .catalog
        .prepare_classic_reconciliation_epoch(
            candidate,
            reconciliation.previous_assignment(),
            reconciliation.previous_classic_generation(),
            reconciliation.replacement_classic_generation(),
        )
        .unwrap_or_else(|| panic!("catalog reconciliation epoch"))
        .commit();
    let added = LiveGroupAssignment::try_new(
        reconciliation.replacement_assignment().group_id(),
        reconciliation.replacement_assignment().member_id(),
        reconciliation
            .replacement_assignment()
            .assignment_generation(),
        reconciliation.delta().added().to_vec(),
    )
    .unwrap_or_else(|error| panic!("added assignment: {error}"));
    let position = prepare_classic_group_position(
        &entry.catalog,
        reconciliation.replacement_cycle(),
        &added,
        OperationDeadline::from_core_for_test(Deadline::from_tick(u64::MAX)),
        Moment::from_tick(10),
    )
    .unwrap_or_else(|error| panic!("added position preparation: {error:?}"));
    let previous = copy_assignment(reconciliation.previous_assignment());
    entry.classic_reconciliation = Some(PreparedClassicGroupReconciliation::new(
        reconciliation,
        previous,
        position,
        Deadline::from_tick(u64::MAX),
    ));
    entry
}

pub(in super::super) fn activate_previous_fetch(entry: &mut GroupConsumerEntry) {
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("previous catalog assignment"));
    let cycle = entry
        .catalog
        .membership_cycle()
        .unwrap_or_else(|| panic!("previous membership cycle"));
    let fence = GroupPositionFence::new(
        entry.group_id(),
        cycle,
        assignment.member_id(),
        assignment.assignment_generation(),
    );
    entry
        .processing_lease
        .prepare_activation(
            ClassicProcessingLeaseFence::new(
                entry.group_id(),
                cycle,
                assignment.assignment_generation(),
            ),
            Moment::from_tick(10),
        )
        .unwrap_or_else(|error| panic!("previous processing lease: {error:?}"))
        .commit();
    let facts = assignment
        .partitions()
        .iter()
        .copied()
        .map(|partition| {
            GroupPositionPartitionFact::committed(
                partition,
                NextFetchOffset::try_from_raw(17).unwrap_or_else(|| panic!("positive next offset")),
            )
        })
        .collect();
    entry
        .fetch
        .try_activate(
            completed_ready(
                fence,
                Moment::from_tick(11),
                GroupPositionBatch::new(0, facts),
            ),
            fence,
        )
        .unwrap_or_else(|_error| panic!("previous Fetch activation"));
}

pub(in super::super) fn defer_rejoin_during_reconciliation(
    entry: &mut GroupConsumerEntry,
) -> ClassicRejoinSchedule {
    let heartbeat = entry
        .classic_reconciliation
        .as_ref()
        .unwrap_or_else(|| panic!("pending cooperative reconciliation"))
        .reconciliation()
        .heartbeat();
    match entry.heartbeat.state() {
        ClassicHeartbeatExecutionState::Dormant => entry
            .heartbeat
            .prepare_install(heartbeat)
            .unwrap_or_else(|error| panic!("reconciliation Heartbeat install: {error:?}"))
            .commit(),
        ClassicHeartbeatExecutionState::Waiting(installed) if *installed == heartbeat => {}
        _ => panic!("reconciliation Heartbeat must remain locally owned"),
    }
    let now = Moment::from_tick(heartbeat.due().tick());
    entry
        .classic
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt: heartbeat.attempt(),
            now,
        })
        .unwrap_or_else(|error| panic!("reconciliation Heartbeat due: {error}"));
    let transition = entry
        .classic
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt: heartbeat.attempt(),
            now,
            error: ClassicBrokerError::try_from_code(27)
                .unwrap_or_else(|| panic!("rebalance-in-progress")),
        })
        .unwrap_or_else(|error| panic!("reconciliation Heartbeat rejection: {error}"));
    install_heartbeat_rejection(entry, transition, now)
        .unwrap_or_else(|_fault| panic!("install deferred reconciliation rejoin"));
    entry
        .heartbeat
        .clear_local()
        .unwrap_or_else(|error| panic!("clear rejected Heartbeat locally: {error:?}"));
    entry
        .rejoin
        .schedule()
        .unwrap_or_else(|| panic!("deferred reconciliation rejoin schedule"))
}

fn copy_assignment(assignment: &LiveGroupAssignment) -> LiveGroupAssignment {
    LiveGroupAssignment::try_new(
        assignment.group_id(),
        assignment.member_id(),
        assignment.assignment_generation(),
        assignment.partitions().to_vec(),
    )
    .unwrap_or_else(|error| panic!("copy reconciliation assignment: {error}"))
}
