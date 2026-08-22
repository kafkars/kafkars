//! Engine retention and heartbeat materialization while Kafka computes an initial assignment.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{ConsumerGroupHeartbeatPhase, Moment};

use crate::{clock::MonotonicClock, driver::TopicPartitionCountFact};

use super::{
    super::{
        classic_group_leave::GroupConsumerCloseCompletion,
        consumer_group_close::complete_consumer_group_leave,
        consumer_group_execution::ConsumerGroupExecution,
        consumer_group_execution_cadence::ConsumerGroupCoordinatorLoadRetryTurn,
        consumer_group_heartbeat_due::settle_consumer_group_load_retry_turn,
        consumer_group_heartbeat_settlement::ConsumerGroupHeartbeatSettlementTurn,
        consumer_group_heartbeat_settlement_test::{
            modern_entry_with_instance, success_with, success_without_assignment,
        },
        consumer_group_heartbeat_submission::prepare_request,
        registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
    },
    settle_success,
};

#[test]
fn assignment_less_success_retains_member_then_installs_generation_one_on_next_heartbeat() {
    let clock = MonotonicClock::new();
    let (mut entry, _initial_now) = awaiting_entry(&clock);
    let member_id = entry
        .catalog
        .current_member_id()
        .unwrap_or_else(|| panic!("retained member identity"));
    let member = entry
        .catalog
        .current_member()
        .cloned()
        .unwrap_or_else(|| panic!("retained member spelling"));
    let cycle = entry
        .catalog
        .membership_cycle()
        .unwrap_or_else(|| panic!("retained membership cycle"));
    assert_eq!(
        entry
            .catalog
            .consumer_group_member_epoch()
            .map(kafka_client_core::ConsumerGroupMemberEpoch::get),
        Some(1)
    );
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.position.is_dormant());
    assert!(entry.processing_lease.active_schedule().is_none());
    assert!(entry.fetch.activation().is_none());
    assert!(entry.catalog.take_event().is_none());

    let schedule = entry
        .consumer
        .as_ref()
        .and_then(|execution| execution.machine().schedule())
        .unwrap_or_else(|| panic!("awaiting cadence"));
    assert!(schedule.assignment_generation().is_none());
    let due = Moment::from_tick(schedule.deadline().tick());
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .prepare_due_heartbeat(due, &clock)
        .unwrap_or_else(|error| panic!("prepare pending heartbeat: {error:?}"));
    let prepared = entry
        .consumer
        .as_ref()
        .and_then(ConsumerGroupExecution::prepared)
        .unwrap_or_else(|| panic!("prepared pending heartbeat"));
    assert_eq!(prepared.assignment_generation(), None);
    let request = prepare_request(&entry)
        .unwrap_or_else(|()| panic!("materialize pending heartbeat"))
        .into_generated_request();
    assert_eq!(request.member_epoch, 1);
    assert!(request.topic_partitions.as_ref().is_some_and(Vec::is_empty));

    assert_eq!(
        settle_success(&mut entry, due, success_with(2, 0)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("installed assignment"));
    assert_eq!(assignment.member_id(), member_id);
    assert_eq!(assignment.assignment_generation().get(), 1);
    assert_eq!(entry.catalog.current_member(), Some(&member));
    assert_eq!(entry.catalog.membership_cycle(), Some(cycle));
    assert_eq!(
        entry
            .consumer
            .as_ref()
            .map(|execution| execution.machine().phase()),
        Some(ConsumerGroupHeartbeatPhase::Stable)
    );
}

#[test]
fn assignment_less_member_materializes_leave_and_clears_retained_catalog_session() {
    let clock = MonotonicClock::new();
    let (mut entry, now) = awaiting_entry(&clock);
    let close = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("close deadline: {error:?}"));
    entry
        .leave
        .begin(
            close.operation_deadline(),
            Arc::new(GroupConsumerCloseCompletion::pending()),
        )
        .unwrap_or_else(|_completion| panic!("close admission"));
    entry.state = GroupConsumerEntryState::Closing;
    assert!(
        entry
            .consumer
            .as_mut()
            .unwrap_or_else(|| panic!("modern execution"))
            .prepare_leave(now, close.operation_deadline())
            .unwrap_or_else(|error| panic!("prepare pending leave: {error:?}"))
    );
    let prepared = entry
        .consumer
        .as_ref()
        .and_then(ConsumerGroupExecution::prepared)
        .unwrap_or_else(|| panic!("prepared leave"));
    assert_eq!(prepared.assignment_generation(), None);
    let request = prepare_request(&entry)
        .unwrap_or_else(|()| panic!("materialize pending leave"))
        .into_generated_request();
    assert_eq!(request.member_epoch, -1);

    complete_consumer_group_leave(&mut entry)
        .unwrap_or_else(|error| panic!("complete pending leave: {error:?}"));
    assert!(entry.catalog.current_member_id().is_none());
    assert_eq!(
        entry
            .consumer
            .as_ref()
            .map(|execution| execution.machine().phase()),
        Some(ConsumerGroupHeartbeatPhase::Closed)
    );
}

#[test]
fn assignment_less_coordinator_load_expiry_clears_the_retained_catalog_member() {
    let clock = MonotonicClock::new();
    let (mut entry, _initial_now) = awaiting_entry(&clock);
    let schedule = entry
        .consumer
        .as_ref()
        .and_then(|execution| execution.machine().schedule())
        .unwrap_or_else(|| panic!("awaiting cadence"));
    let due = Moment::from_tick(schedule.deadline().tick());
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .prepare_due_heartbeat(due, &clock)
        .unwrap_or_else(|error| panic!("prepare pending heartbeat: {error:?}"));
    let deadline = entry
        .consumer
        .as_ref()
        .and_then(ConsumerGroupExecution::prepared)
        .unwrap_or_else(|| panic!("prepared pending heartbeat"))
        .deadline()
        .core();
    let response_now = Moment::from_tick(deadline.tick() - 50_000_000);
    let scheduled = entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .schedule_current_coordinator_load_retry(
            response_now,
            kafka_client_core::ConsumerGroupHeartbeatFailure::Broker(14),
        )
        .unwrap_or_else(|error| panic!("schedule load retry: {error:?}"));
    assert!(matches!(
        scheduled,
        ConsumerGroupCoordinatorLoadRetryTurn::Scheduled { schedule }
            if schedule.not_before() == deadline
    ));
    let terminal = entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .prepare_due_coordinator_load_retry(Moment::from_tick(deadline.tick()))
        .unwrap_or_else(|error| panic!("expire load retry: {error:?}"));
    assert!(matches!(
        terminal,
        ConsumerGroupCoordinatorLoadRetryTurn::Terminal {
            kind: kafka_client_core::ConsumerGroupHeartbeatRequestKind::Steady,
            revoked: None,
        }
    ));
    settle_consumer_group_load_retry_turn(&mut entry, terminal)
        .unwrap_or_else(|error| panic!("settle load terminal: {error:?}"));
    assert!(entry.catalog.current_member_id().is_none());
    assert!(entry.consumer_revocation.is_none());
    assert_eq!(
        entry
            .consumer
            .as_ref()
            .map(|execution| execution.machine().phase()),
        Some(ConsumerGroupHeartbeatPhase::Fatal)
    );
}

fn awaiting_entry(clock: &MonotonicClock) -> (GroupConsumerEntry, Moment) {
    let mut entry = modern_entry_with_instance(None);
    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("join deadline: {error:?}"));
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    let topic_id = entry
        .catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("topic identity"));
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .topic_identities_mut()
        .append(
            topic_id,
            TopicPartitionCountFact {
                metadata_generation: 1,
                logical_partition_count: 3,
                kafka_topic_id: Some([7; 16]),
            },
        )
        .unwrap_or_else(|error| panic!("topic fact: {error:?}"));
    assert_eq!(
        settle_success(&mut entry, capture.now(), success_without_assignment(1),),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    (entry, capture.now())
}
