//! Position-terminal gating and retained-member cooperative follow-up Join scenarios.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupTiming, ClassicProtocol, Deadline, GroupId, GroupPositionBatch,
    GroupPositionBootstrapEffect, GroupPositionBootstrapInput, GroupPositionBootstrapTerminal,
    GroupPositionPartitionFact, MemberId, MembershipCycle, Moment,
};

use std::time::Instant;

use crate::clock::{MonotonicClock, OperationDeadline};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_position::{
        ClassicGroupPositionCompleted, ClassicGroupPositionExecutionState,
        ClassicGroupPositionFailure, ClassicGroupPositionPreparation,
    },
    classic_group_reconciliation_turn::{
        ClassicGroupReconciliationTurn, followup_join_matches, prepare_retained_followup_join,
    },
    registry::GroupConsumerRegistry,
    registry_event_reconciliation_test::{activate_previous_fetch, prepared_reconciliation},
    registry_test_support::stop_registry,
};

#[test]
fn pending_fetch_effect_blocks_revocation_without_consuming_owners() {
    let mut entry = prepared_reconciliation();
    activate_previous_fetch(&mut entry);
    entry
        .classic_reconciliation
        .as_mut()
        .unwrap_or_else(|| panic!("prepared cooperative reconciliation"))
        .confirm_sync();
    let mut registry = GroupConsumerRegistry::start()
        .unwrap_or_else(|error| panic!("cooperative registry: {error}"));
    registry.retained_group_bytes = entry.group_bytes();
    registry.next_group_id = GroupId::try_from_raw(2);
    registry.entries.push(entry);

    assert_eq!(
        registry.stage_one_classic_group_reconciliation(Moment::from_tick(20)),
        Ok(ClassicGroupReconciliationTurn::Blocked)
    );
    assert!(registry.entries[0].classic_reconciliation.is_some());
    assert!(registry.entries[0].revocation.is_dormant());
    stop_registry(&mut registry);
}

#[test]
fn non_ready_cooperative_position_terminalizes_before_reconciliation_finish() {
    let mut entry = prepared_reconciliation();
    let group_id = entry.group_id();
    let completed = {
        let pending = entry
            .classic_reconciliation
            .as_mut()
            .unwrap_or_else(|| panic!("prepared cooperative reconciliation"));
        pending.confirm_sync();
        pending.stage_revocation();
        pending.settle_revocation();
        let partition = pending
            .reconciliation()
            .delta()
            .added()
            .first()
            .copied()
            .unwrap_or_else(|| panic!("added cooperative partition"));
        let ClassicGroupPositionPreparation::Prepared(prepared) = pending
            .take_position()
            .unwrap_or_else(|| panic!("prepared replacement position"))
        else {
            panic!("nonempty replacement position must require OffsetFetch");
        };
        let (key, mut machine, correlation, request, result_buffer) = prepared.into_parts();
        drop((correlation, request, result_buffer));
        let fence = key.fence();
        let operation_deadline = key.operation_deadline();
        machine
            .apply(GroupPositionBootstrapInput::DriverAccepted { fence })
            .unwrap_or_else(|error| panic!("position driver acceptance: {error}"));
        let transition = machine
            .apply(GroupPositionBootstrapInput::OffsetsFetched {
                fence,
                now: Moment::from_tick(20),
                batch: GroupPositionBatch::new(
                    0,
                    vec![GroupPositionPartitionFact::missing(partition)],
                ),
            })
            .unwrap_or_else(|error| panic!("missing-offset terminal: {error}"));
        let Some(GroupPositionBootstrapEffect::Complete { terminal, .. }) =
            transition.into_effect()
        else {
            panic!("missing offset must complete position bootstrap");
        };
        assert!(matches!(
            &terminal,
            GroupPositionBootstrapTerminal::MissingOffsets(_)
        ));
        ClassicGroupPositionCompleted::new_with_operation_deadline(
            machine,
            terminal,
            Moment::from_tick(20),
            operation_deadline,
        )
    };
    entry
        .position
        .set(ClassicGroupPositionExecutionState::Complete(completed));

    let retained_bytes = entry.group_bytes();
    let mut registry = GroupConsumerRegistry::start()
        .unwrap_or_else(|error| panic!("cooperative registry: {error}"));
    registry.retained_group_bytes = retained_bytes;
    registry.next_group_id = GroupId::try_from_raw(2);
    registry.entries.push(entry);

    assert_eq!(
        registry.finish_one_classic_group_reconciliation(
            Moment::from_tick(21),
            &MonotonicClock::new(),
        ),
        Ok(ClassicGroupReconciliationTurn::Idle),
        "membership must leave a non-Ready position terminal to its position owner"
    );
    assert!(registry.terminalize_one_classic_group_position_failure());
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("cooperative entry"));
    assert!(matches!(
        entry.fault.as_ref(),
        Some(ClassicGroupEntryFault::PositionFailure(
            ClassicGroupPositionFailure::Bootstrap(completed)
        )) if matches!(
            completed.terminal(),
            GroupPositionBootstrapTerminal::MissingOffsets(_)
        )
    ));

    drop(registry.entries.pop());
    registry.retained_group_bytes = 0;
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("empty registry recovery: {error}"));
    registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("empty registry finish: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

#[test]
fn exact_catalog_and_replacement_member_accept_the_next_cycle_join() {
    let group = group_id(1);
    let member = member_id(1);
    let replacement_cycle = cycle(2);
    let first = join(group, cycle(3), Some(member));

    assert!(followup_join_matches(
        true,
        group,
        Some(member),
        replacement_cycle,
        member,
        Some(&first),
        None,
    ));
    let prepared = prepare_retained_followup_join(
        group,
        cycle(3),
        ClassicProtocol::CooperativeSticky,
        member,
        timing(),
        OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now()),
    );
    assert_eq!(prepared.member_id(), Some(member));
}

#[test]
fn absent_or_mismatched_retained_member_rejects_without_weakening_identity() {
    let group = group_id(1);
    let member = member_id(1);
    let other = member_id(2);
    let replacement_cycle = cycle(2);
    let absent = join(group, cycle(3), None);
    let retained = join(group, cycle(3), Some(member));

    assert!(!followup_join_matches(
        true,
        group,
        Some(member),
        replacement_cycle,
        member,
        Some(&absent),
        None,
    ));
    assert!(!followup_join_matches(
        true,
        group,
        Some(other),
        replacement_cycle,
        member,
        Some(&retained),
        None,
    ));
    assert!(!followup_join_matches(
        true,
        group,
        Some(member),
        replacement_cycle,
        other,
        Some(&retained),
        None,
    ));
}

#[test]
fn wrong_group_cycle_or_effect_count_preserves_the_existing_shape_fence() {
    let group = group_id(1);
    let member = member_id(1);
    let replacement_cycle = cycle(2);
    let wrong_group = join(group_id(2), cycle(3), Some(member));
    let wrong_cycle = join(group, cycle(4), Some(member));
    let exact = join(group, cycle(3), Some(member));
    let extra = join(group, cycle(3), Some(member));

    for first in [&wrong_group, &wrong_cycle] {
        assert!(!followup_join_matches(
            true,
            group,
            Some(member),
            replacement_cycle,
            member,
            Some(first),
            None,
        ));
    }
    assert!(!followup_join_matches(
        true,
        group,
        Some(member),
        replacement_cycle,
        member,
        Some(&exact),
        Some(&extra),
    ));
    assert!(!followup_join_matches(
        false,
        group,
        Some(member),
        replacement_cycle,
        member,
        Some(&exact),
        None,
    ));
    assert!(followup_join_matches(
        false,
        group,
        Some(member),
        replacement_cycle,
        member,
        None,
        None,
    ));
}

fn join(
    group_id: GroupId,
    cycle: MembershipCycle,
    member_id: Option<MemberId>,
) -> ClassicGroupEffect {
    ClassicGroupEffect::Join {
        group_id,
        cycle,
        protocol: ClassicProtocol::CooperativeSticky,
        member_id,
        timing: timing(),
        deadline: Deadline::from_tick(100),
    }
}

fn timing() -> ClassicGroupTiming {
    ClassicGroupTiming::try_new(10_000, 30_000).unwrap_or_else(|error| panic!("timing: {error}"))
}

fn group_id(value: u64) -> GroupId {
    GroupId::try_from_raw(value).unwrap_or_else(|| panic!("group"))
}

fn member_id(value: u64) -> MemberId {
    MemberId::try_from_raw(value).unwrap_or_else(|| panic!("member"))
}

fn cycle(value: u64) -> MembershipCycle {
    MembershipCycle::try_from_raw(value).unwrap_or_else(|| panic!("cycle"))
}
