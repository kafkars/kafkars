//! KIP-848 assignment reconciliation through retirement and acknowledgement.

use kafka_client_core::{
    GroupPositionBatch, GroupPositionFence, GroupPositionPartitionFact, Moment, NextFetchOffset,
};

use crate::clock::MonotonicClock;

use super::{
    classic_group_fetch::{
        ClassicGroupFetchTransferTurn, transfer_completed_consumer_group_position,
    },
    classic_group_position::{ClassicGroupPositionExecutionState, test_support::completed_ready},
    consumer_group_assignment_retirement::{
        ConsumerGroupAssignmentRetirementTurn, retire_entry_assignment,
    },
    consumer_group_heartbeat_settlement::{ConsumerGroupHeartbeatSettlementTurn, settle_success},
    consumer_group_heartbeat_settlement_test::{
        installed_modern_entry, success_with, success_without_assignment,
    },
    consumer_group_heartbeat_submission::prepare_request,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the scenario proves the full retire acknowledge and install sequence under one membership epoch change"
)]
fn changed_member_epoch_retires_then_installs_the_new_assignment() {
    let (mut entry, topic_id) = installed_modern_entry();
    activate_fetch(&mut entry);
    let clock = MonotonicClock::new();
    let schedule = entry
        .consumer
        .as_ref()
        .and_then(|consumer| consumer.machine().schedule())
        .unwrap_or_else(|| panic!("heartbeat schedule"));
    let now = Moment::from_tick(schedule.deadline().tick());
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .prepare_due_heartbeat(now, &clock)
        .unwrap_or_else(|error| panic!("prepare heartbeat: {error:?}"));
    assert_eq!(
        settle_success(&mut entry, now, success_with(2, 1)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    assert!(entry.consumer_revocation.is_none());
    assert!(entry.consumer_reconciliation.is_some());
    let revocation_deadline = entry
        .revocation
        .next_deadline()
        .unwrap_or_else(|| panic!("graceful revocation deadline"));
    assert_eq!(revocation_deadline.tick(), now.tick() + 30_000_000_000);
    assert_eq!(
        entry
            .catalog
            .consumer_group_member_epoch()
            .map(kafka_client_core::ConsumerGroupMemberEpoch::get),
        Some(2)
    );
    assert_eq!(
        entry
            .catalog
            .live_assignment()
            .and_then(|assignment| assignment.partitions().first())
            .map(|partition| partition.partition().get()),
        Some(0)
    );
    let revocation_epoch = entry
        .revocation
        .active_assignment_epoch()
        .unwrap_or_else(|| panic!("active revocation epoch"));
    match entry.catalog.take_event() {
        Some(crate::consumer::GroupConsumerEvent::PartitionsRevoked(assignment)) => {
            assert_eq!(assignment.assignment_epoch(), revocation_epoch.get());
        }
        _ => panic!("graceful revocation event"),
    }

    let steady = entry
        .consumer
        .as_ref()
        .and_then(|consumer| consumer.machine().schedule())
        .unwrap_or_else(|| panic!("steady heartbeat schedule"));
    let steady_now = Moment::from_tick(steady.deadline().tick());
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .prepare_due_heartbeat(steady_now, &clock)
        .unwrap_or_else(|error| panic!("prepare steady heartbeat: {error:?}"));
    let request = prepare_request(&entry)
        .unwrap_or_else(|()| panic!("prepare old-owned heartbeat"))
        .into_generated_request();
    assert_eq!(request.member_epoch, 2);
    assert_eq!(
        request
            .topic_partitions
            .as_ref()
            .and_then(|topics| topics.first())
            .and_then(|topic| topic.partitions.first())
            .copied(),
        Some(0)
    );
    assert_eq!(
        settle_success(&mut entry, steady_now, success_without_assignment(2)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    assert!(entry.consumer_revocation.is_none());
    assert!(entry.consumer_reconciliation.is_some());
    assert!(entry.catalog.take_event().is_none());

    entry
        .revocation
        .acknowledge(revocation_epoch, steady_now)
        .unwrap_or_else(|error| panic!("acknowledgment: {error:?}"));
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);
    assert_eq!(
        registry.turn_graceful_revocation(steady_now),
        Ok(super::classic_group_graceful_revocation::ClassicGroupRevocationTurn::Progress)
    );
    let entry = registry
        .entries
        .first_mut()
        .unwrap_or_else(|| panic!("modern entry"));
    assert!(entry.revocation.is_dormant());
    assert!(entry.consumer_revocation.is_some());

    assert_eq!(
        retire_entry_assignment(entry, steady_now, &clock),
        Ok(ConsumerGroupAssignmentRetirementTurn::Progress)
    );
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.consumer_reconciliation.is_some());
    let ack = entry
        .consumer
        .as_ref()
        .and_then(super::consumer_group_execution::ConsumerGroupExecution::prepared)
        .unwrap_or_else(|| panic!("empty-owned acknowledgement"));
    assert_eq!(ack.assignment_generation(), None);
    let request = prepare_request(entry)
        .unwrap_or_else(|()| panic!("prepare empty-owned acknowledgement"))
        .into_generated_request();
    assert_eq!(request.member_epoch, 2);
    assert!(request.topic_partitions.as_ref().is_some_and(Vec::is_empty));
    assert_eq!(
        settle_success(entry, steady_now, success_without_assignment(2)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("replacement assignment"));
    assert_eq!(assignment.assignment_generation().get(), 2);
    assert_eq!(assignment.partitions()[0].topic_id(), topic_id);
    assert_eq!(assignment.partitions()[0].partition().get(), 1);
    assert!(entry.consumer_reconciliation.is_none());
    let ClassicGroupPositionExecutionState::Prepared(position) = entry.position.state() else {
        panic!("replacement position resolution")
    };
    assert_eq!(position.key().operation_deadline(), ack.deadline());
    assert!(matches!(
        entry.catalog.take_event(),
        Some(crate::consumer::GroupConsumerEvent::PartitionsAssigned(_))
    ));
    assert!(entry.catalog.take_event().is_none());
}

#[test]
fn exact_rebalance_deadline_authorizes_existing_retirement_path() {
    let (mut entry, _topic_id) = installed_modern_entry();
    activate_fetch(&mut entry);
    let clock = MonotonicClock::new();
    let schedule = entry
        .consumer
        .as_ref()
        .and_then(|consumer| consumer.machine().schedule())
        .unwrap_or_else(|| panic!("heartbeat schedule"));
    let now = Moment::from_tick(schedule.deadline().tick());
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .prepare_due_heartbeat(now, &clock)
        .unwrap_or_else(|error| panic!("prepare heartbeat: {error:?}"));
    assert_eq!(
        settle_success(&mut entry, now, success_with(2, 1)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    let deadline = entry
        .revocation
        .next_deadline()
        .unwrap_or_else(|| panic!("revocation deadline"));
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(deadline.tick())),
        Ok(super::classic_group_graceful_revocation::ClassicGroupRevocationTurn::Progress)
    );
    let entry = registry
        .entries
        .first()
        .unwrap_or_else(|| panic!("modern entry"));
    assert!(entry.revocation.terminal().is_some());
    assert!(entry.consumer_revocation.is_none());
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(deadline.tick())),
        Ok(super::classic_group_graceful_revocation::ClassicGroupRevocationTurn::Progress)
    );
    let entry = registry
        .entries
        .first_mut()
        .unwrap_or_else(|| panic!("modern entry"));
    assert!(entry.revocation.is_dormant());
    assert!(entry.consumer_revocation.is_some());
    assert_eq!(
        retire_entry_assignment(entry, Moment::from_tick(deadline.tick()), &clock),
        Ok(ConsumerGroupAssignmentRetirementTurn::Progress)
    );
    assert!(matches!(
        entry.catalog.take_event(),
        Some(crate::consumer::GroupConsumerEvent::PartitionsLost(_))
    ));
}

#[test]
fn replacement_before_fetch_activation_uses_immediate_retirement() {
    let (mut entry, _topic_id) = installed_modern_entry();
    assert!(entry.processing_lease.active_schedule().is_some());
    assert!(entry.fetch.activation().is_none());
    let clock = MonotonicClock::new();
    let schedule = entry
        .consumer
        .as_ref()
        .and_then(|consumer| consumer.machine().schedule())
        .unwrap_or_else(|| panic!("heartbeat schedule"));
    let now = Moment::from_tick(schedule.deadline().tick());
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .prepare_due_heartbeat(now, &clock)
        .unwrap_or_else(|error| panic!("prepare heartbeat: {error:?}"));
    assert_eq!(
        settle_success(&mut entry, now, success_with(2, 1)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    assert!(entry.revocation.is_dormant());
    assert!(entry.consumer_revocation.is_some());
    assert!(entry.consumer_reconciliation.is_some());
}

fn activate_fetch(entry: &mut GroupConsumerEntry) {
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("installed assignment"));
    let partition = assignment.partitions()[0];
    let fence = GroupPositionFence::new(
        assignment.group_id(),
        entry
            .consumer
            .as_ref()
            .and_then(super::consumer_group_execution::ConsumerGroupExecution::cycle)
            .unwrap_or_else(|| panic!("membership cycle")),
        assignment.member_id(),
        assignment.assignment_generation(),
    );
    entry
        .position
        .set(ClassicGroupPositionExecutionState::Complete(
            completed_ready(
                fence,
                Moment::from_tick(9),
                GroupPositionBatch::new(
                    0,
                    vec![GroupPositionPartitionFact::committed(
                        partition,
                        NextFetchOffset::try_from_raw(17)
                            .unwrap_or_else(|| panic!("next Fetch offset")),
                    )],
                ),
            ),
        ));
    assert_eq!(
        transfer_completed_consumer_group_position(
            entry
                .consumer
                .as_ref()
                .unwrap_or_else(|| panic!("modern execution")),
            &entry.catalog,
            &mut entry.position,
            &mut entry.fetch,
        ),
        Ok(ClassicGroupFetchTransferTurn::Activated)
    );
}
