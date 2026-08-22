//! Join submission readiness waits for every bounded topic identity.

use std::time::Duration;

use kafka_client_core::{
    GroupId, GroupPositionBatch, GroupPositionPartitionFact, MembershipCycle, Moment, TopicId,
};

use crate::{
    clock::MonotonicClock, consumer::GroupConsumerPositionFailureKind,
    driver::TopicPartitionCountFact,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_fetch::current_consumer_group_position_fence,
    classic_group_position::{ClassicGroupPositionExecutionState, test_support::completed_ready},
    consumer_group_close::position_failure_allows_consumer_group_leave,
    consumer_group_execution::ConsumerGroupExecution,
    consumer_group_execution_fencing::{
        consumer_group_execution_is_ready, consumer_group_heartbeat_is_ready,
    },
    consumer_group_heartbeat_settlement_test::installed_modern_entry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

#[test]
fn join_waits_until_all_topic_uuids_are_retained() {
    let mut execution = ConsumerGroupExecution::try_new(group_id(), 1, 30_000)
        .unwrap_or_else(|error| panic!("execution: {error:?}"));
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    assert!(!consumer_group_execution_is_ready(&execution));
    execution
        .topic_identities_mut()
        .append(
            TopicId::from_raw(1),
            TopicPartitionCountFact {
                metadata_generation: 1,
                logical_partition_count: 3,
                kafka_topic_id: Some([4; 16]),
            },
        )
        .unwrap_or_else(|error| panic!("identity: {error:?}"));
    assert!(consumer_group_execution_is_ready(&execution));
}

#[test]
fn only_a_closing_position_failure_allows_consumer_group_leave_submission() {
    let (mut entry, _now) = position_faulted_closing_leave_entry();

    assert!(position_failure_allows_consumer_group_leave(&entry));
    assert!(consumer_group_heartbeat_is_ready(&entry));
    assert_eq!(
        entry.position_failure_observation,
        Some(GroupConsumerPositionFailureKind::MissingOffset)
    );
    assert!(matches!(
        &entry.fault,
        Some(ClassicGroupEntryFault::PositionFailure(_))
    ));

    drop(entry.fault.take());
    entry.fault = Some(ClassicGroupEntryFault::SyncRecoverySemantic(
        MembershipCycle::try_from_raw(9).unwrap_or_else(|| panic!("membership cycle")),
    ));
    assert!(!position_failure_allows_consumer_group_leave(&entry));
    assert!(!consumer_group_heartbeat_is_ready(&entry));
}

pub(super) fn position_faulted_closing_leave_entry() -> (GroupConsumerEntry, Moment) {
    let (mut entry, _topic_id) = installed_modern_entry();
    let consumer = entry
        .consumer
        .as_ref()
        .unwrap_or_else(|| panic!("consumer execution"));
    let fence = current_consumer_group_position_fence(consumer, &entry.catalog)
        .unwrap_or_else(|error| panic!("position fence: {error:?}"));
    let partition = entry
        .catalog
        .live_assignment()
        .and_then(|assignment| assignment.partitions().first())
        .copied()
        .unwrap_or_else(|| panic!("assigned partition"));
    entry
        .position
        .set(ClassicGroupPositionExecutionState::Complete(
            completed_ready(
                fence,
                Moment::from_tick(9),
                GroupPositionBatch::new(0, vec![GroupPositionPartitionFact::missing(partition)]),
            ),
        ));
    let failure = entry
        .position
        .take_failure()
        .unwrap_or_else(|| panic!("missing-offset position failure"));
    let observation = failure.observation_kind();
    entry.position_failure_observation = Some(observation);
    entry.fault = Some(ClassicGroupEntryFault::PositionFailure(failure));
    entry.state = GroupConsumerEntryState::Closing;
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("close capture: {error}"));
    assert_eq!(
        entry
            .consumer
            .as_mut()
            .unwrap_or_else(|| panic!("consumer execution"))
            .prepare_leave(capture.now(), capture.operation_deadline()),
        Ok(true)
    );
    (entry, capture.now())
}

fn group_id() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"))
}
