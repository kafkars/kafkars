//! Registry staging, event observation, and terminal retirement scenarios.

use kafka_client_core::{
    AssignmentEpoch, ClassicGroupEffect, ClassicGroupInput, Deadline, GroupId, GroupPositionBatch,
    GroupPositionFence, GroupPositionPartitionFact, Moment, NextFetchOffset,
};

use crate::consumer::GroupConsumerEvent;

use super::{
    classic_group_graceful_revocation::ClassicGroupRevocationTurn,
    classic_group_position::test_support::completed_ready,
    registry::GroupConsumerRegistry,
    registry_event_reconciliation_test::prepared_reconciliation,
    registry_graceful_revocation::{
        stage_classic_group_reconciliation_revocation, stage_classic_group_revocation,
    },
    registry_test_support::{
        install_ready_group_delivery, install_session, register, started_registry,
    },
};

#[test]
fn observed_ack_retains_terminal_until_exact_assignment_retirement() {
    let deadline = Deadline::from_tick(90);
    let (mut registry, group_id, epoch) = staged_registry(deadline);

    let event = registry
        .take_event(group_id)
        .unwrap_or_else(|error| panic!("event observation: {error:?}"))
        .unwrap_or_else(|| panic!("one revocation event expected"));
    let GroupConsumerEvent::PartitionsRevoked(assignment) = event else {
        panic!("revocation event expected");
    };
    assert_eq!(assignment.assignment_epoch(), epoch.get());
    registry
        .acknowledge_revocation(group_id, epoch.get(), Moment::from_tick(89))
        .unwrap_or_else(|error| panic!("completion: {error:?}"));

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(entry.catalog.live_assignment().is_some());
    assert!(entry.revocation.terminal().is_some());
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(89)),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    assert_retired(&registry, group_id);
}

#[test]
fn exact_deadline_loses_then_retires_on_the_next_bounded_turn() {
    let deadline = Deadline::from_tick(90);
    let (mut registry, group_id, _epoch) = staged_registry(deadline);

    assert_eq!(registry.graceful_revocation_next_deadline(), Some(deadline));
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(90)),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    assert!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.catalog.live_assignment())
            .is_some()
    );
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(90)),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    assert_retired(&registry, group_id);
}

#[test]
fn cooperative_deadline_loss_publishes_only_the_removed_partition() {
    let deadline = Deadline::from_tick(90);
    let (mut registry, group_id, epoch) = staged_cooperative_registry(deadline);

    assert_partition_event(registry.take_event(group_id), false, epoch, 1);
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(90)),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(90)),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    assert_partition_event(registry.take_event(group_id), true, epoch, 1);
    assert_reconciliation_revocation_settled(&registry, group_id);
}

#[test]
fn cooperative_owner_loss_replaces_unobserved_revoke_with_exact_loss() {
    let (mut registry, group_id, epoch) = staged_cooperative_registry(Deadline::from_tick(90));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("cooperative entry expected"));
    assert_eq!(entry.revocation.lose_owner(), Ok(true));

    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(50)),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    assert_partition_event(registry.take_event(group_id), true, epoch, 1);
    assert_eq!(registry.take_event(group_id), Ok(None));
    assert_reconciliation_revocation_settled(&registry, group_id);
}

fn staged_registry(deadline: Deadline) -> (GroupConsumerRegistry, GroupId, AssignmentEpoch) {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    install_ready_group_delivery(&mut registry, group_id, 17);
    let (assignment, generation, epoch) = {
        let entry = registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .unwrap_or_else(|| panic!("registered entry expected"));
        entry.catalog.stage_installed_assignment_event();
        entry.catalog.confirm_sync_event();
        let epoch = entry
            .fetch
            .activation()
            .unwrap_or_else(|| panic!("Fetch activation expected"))
            .binding()
            .assignment_epoch();
        let cycle = entry
            .classic
            .machine()
            .active_cycle()
            .unwrap_or_else(|| panic!("active cycle expected"));
        let effect = entry
            .classic
            .apply(ClassicGroupInput::AssignmentLost { cycle })
            .unwrap_or_else(|error| panic!("assignment loss: {error}"))
            .into_effects()
            .next()
            .unwrap_or_else(|| panic!("Revoke effect expected"));
        let ClassicGroupEffect::Revoke {
            assignment,
            classic_generation,
        } = effect
        else {
            panic!("Revoke effect expected");
        };
        (assignment, classic_generation, epoch)
    };
    {
        let entry = registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .unwrap_or_else(|| panic!("registered entry expected"));
        stage_classic_group_revocation(
            &mut entry.catalog,
            &entry.fetch,
            &mut entry.revocation,
            assignment,
            generation,
            deadline,
            Moment::from_tick(50),
        )
        .unwrap_or_else(|(error, _assignment)| panic!("staging: {error:?}"));
    }
    (registry, group_id, epoch)
}

fn staged_cooperative_registry(deadline: Deadline) -> (GroupConsumerRegistry, GroupId, u64) {
    let mut entry = prepared_reconciliation();
    let group_id = entry.group_id();
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("previous cooperative assignment"));
    let cycle = entry
        .catalog
        .membership_cycle()
        .unwrap_or_else(|| panic!("previous cooperative cycle"));
    let fence = GroupPositionFence::new(
        group_id,
        cycle,
        assignment.member_id(),
        assignment.assignment_generation(),
    );
    let position_facts = assignment
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
                Moment::from_tick(49),
                GroupPositionBatch::new(0, position_facts),
            ),
            fence,
        )
        .unwrap_or_else(|_error| panic!("previous Fetch activation"));
    let epoch = entry
        .fetch
        .activation()
        .unwrap_or_else(|| panic!("previous Fetch binding"))
        .binding()
        .assignment_epoch()
        .get();
    let (assignment, generation, removed) = {
        let pending = entry
            .classic_reconciliation
            .as_mut()
            .unwrap_or_else(|| panic!("prepared cooperative reconciliation"));
        pending.confirm_sync();
        let generation = pending.reconciliation().previous_classic_generation();
        let removed = pending.reconciliation().delta().removed().to_vec();
        let assignment = pending
            .take_revocation_assignment()
            .unwrap_or_else(|| panic!("prepared revocation assignment"));
        (assignment, generation, removed)
    };
    stage_classic_group_reconciliation_revocation(
        &mut entry.catalog,
        &entry.fetch,
        &mut entry.revocation,
        assignment,
        generation,
        &removed,
        deadline,
        Moment::from_tick(50),
    )
    .unwrap_or_else(|(error, _assignment)| panic!("cooperative staging: {error:?}"));
    entry
        .classic_reconciliation
        .as_mut()
        .unwrap_or_else(|| panic!("prepared cooperative reconciliation"))
        .stage_revocation();

    let retained_group_bytes = entry.group_bytes();
    let mut registry = GroupConsumerRegistry::start()
        .unwrap_or_else(|error| panic!("cooperative registry: {error}"));
    registry.retained_group_bytes = retained_group_bytes;
    registry.next_group_id = GroupId::try_from_raw(2);
    registry.entries.push(entry);
    (registry, group_id, epoch)
}

fn assert_partition_event(
    event: Result<Option<GroupConsumerEvent>, super::registry_event::GroupConsumerEventError>,
    expected_loss: bool,
    epoch: u64,
    partition: i32,
) {
    let event = event.unwrap_or_else(|error| panic!("assignment observation: {error:?}"));
    let ((false, Some(GroupConsumerEvent::PartitionsRevoked(assignment)))
    | (true, Some(GroupConsumerEvent::PartitionsLost(assignment)))) = (expected_loss, event)
    else {
        panic!("expected exact revocation disposition");
    };
    assert_eq!(assignment.assignment_epoch(), epoch);
    assert_eq!(assignment.partitions().len(), 1);
    assert_eq!(assignment.partitions()[0].topic(), "orders");
    assert_eq!(assignment.partitions()[0].partition(), partition);
}

fn assert_reconciliation_revocation_settled(registry: &GroupConsumerRegistry, group_id: GroupId) {
    let pending = registry
        .entry(group_id)
        .and_then(|entry| entry.classic_reconciliation.as_ref())
        .unwrap_or_else(|| panic!("cooperative reconciliation remains retained"));
    assert!(pending.revocation_is_settled());
    assert!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.catalog.live_assignment())
            .is_some()
    );
}

fn assert_retired(registry: &GroupConsumerRegistry, group_id: GroupId) {
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.fetch.machine_assignment_epoch().is_none());
    assert!(entry.revocation.is_dormant());
    assert_eq!(registry.graceful_revocation_unsettled(), 0);
}
