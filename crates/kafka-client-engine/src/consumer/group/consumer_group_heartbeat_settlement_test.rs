//! KIP-848 assignment installation into position, liveness, and catalog owners.

use std::{sync::Arc, time::Duration};

use crate::{
    clock::MonotonicClock,
    config::{ValidatedConsumerFetchConfig, ValidatedConsumerLimits},
    driver::TopicPartitionCountFact,
    protocol::consumer::{
        consumer_group_heartbeat_success_for_test,
        consumer_group_heartbeat_success_without_assignment_for_test,
    },
};
use kafka_client_core::{
    GroupId, GroupPositionBatch, GroupPositionFence, GroupPositionPartitionFact, Moment,
    NextFetchOffset,
};

use super::{
    classic_group_fetch::{
        ClassicGroupFetchTransferTurn, transfer_completed_consumer_group_position,
    },
    classic_group_position::{ClassicGroupPositionExecutionState, test_support::completed_ready},
    classic_group_test_support,
    consumer_group_assignment_retirement::{
        ConsumerGroupAssignmentRetirementTurn, retire_entry_assignment,
        stage_consumer_group_revocation,
    },
    consumer_group_heartbeat_settlement::{ConsumerGroupHeartbeatSettlementTurn, settle_success},
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, default_classic_processing_lease_policy},
    registry_processing::GroupConsumerProcessingTurn,
};
use crate::consumer::GroupConsumerProtocol;

#[test]
fn initial_assignment_owns_position_processing_and_observation_before_fetch() {
    let (mut entry, topic_id) = installed_modern_entry();

    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("installed catalog assignment"));
    assert_eq!(assignment.partitions().len(), 1);
    assert_eq!(assignment.partitions()[0].topic_id(), topic_id);
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::Prepared(_)
    ));
    let schedule = entry
        .processing_lease
        .active_schedule()
        .unwrap_or_else(|| panic!("processing lease"));
    assert_eq!(
        schedule.fence().assignment_generation(),
        assignment.assignment_generation()
    );
    assert!(entry.fetch.activation().is_none());

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
    assert!(entry.fetch.activation().is_some());
    assert!(matches!(
        entry.catalog.take_event(),
        Some(crate::consumer::GroupConsumerEvent::PartitionsAssigned(_))
    ));

    let revoked = entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .close_locally()
        .unwrap_or_else(|error| panic!("local close: {error:?}"));
    stage_consumer_group_revocation(&mut entry, revoked)
        .unwrap_or_else(|error| panic!("stage revocation: {error:?}"));
    assert_eq!(
        retire_entry_assignment(&mut entry, Moment::from_tick(10), &MonotonicClock::new(),),
        Ok(ConsumerGroupAssignmentRetirementTurn::Progress)
    );
    assert!(entry.consumer_revocation.is_none());
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.processing_lease.active_schedule().is_none());
    assert!(entry.fetch.activation().is_none());
    assert!(entry.catalog.take_event().is_none());
}

#[test]
fn processing_expiry_closes_modern_membership_before_retiring_assignment() {
    let (entry, _topic_id) = installed_modern_entry();
    let deadline = entry
        .processing_lease
        .active_schedule()
        .unwrap_or_else(|| panic!("processing schedule"))
        .deadline();
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);

    assert_eq!(
        registry.turn_processing(Moment::from_tick(deadline.tick())),
        Ok(GroupConsumerProcessingTurn::Progress)
    );
    let entry = registry
        .entries
        .first()
        .unwrap_or_else(|| panic!("modern entry"));
    assert!(entry.consumer_revocation.is_some());
    assert!(entry.processing_lease.pending_expiration().is_some());
    assert_eq!(
        entry
            .consumer
            .as_ref()
            .map(|consumer| consumer.machine().phase()),
        Some(kafka_client_core::ConsumerGroupHeartbeatPhase::Closed)
    );
}

pub(super) fn installed_modern_entry() -> (GroupConsumerEntry, kafka_client_core::TopicId) {
    installed_modern_entry_with_instance(None)
}

pub(super) fn installed_modern_entry_with_instance(
    group_instance_id: Option<&Arc<str>>,
) -> (GroupConsumerEntry, kafka_client_core::TopicId) {
    let mut entry = modern_entry_with_instance(group_instance_id);
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error}"));
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
        settle_success(&mut entry, capture.now(), success()),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    (entry, topic_id)
}

pub(super) fn modern_entry_with_instance(
    group_instance_id: Option<&Arc<str>>,
) -> GroupConsumerEntry {
    GroupConsumerEntry::try_new_with_protocol_configuration(
        GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group identity")),
        &Arc::from("workers"),
        group_instance_id,
        &[Arc::from("orders")],
        GroupConsumerProtocol::Consumer,
        crate::consumer::group_registration_request::GroupConsumerClassicAssignor::Range,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
        kafka_client_core::GroupPositionMissingOffsetPolicy::Error,
        kafka_client_core::ReadIsolation::ReadUncommitted,
        default_classic_processing_lease_policy(),
        ValidatedConsumerFetchConfig::default(),
        ValidatedConsumerLimits::default(),
    )
    .unwrap_or_else(|error| panic!("modern entry: {error:?}"))
}

fn success() -> crate::protocol::consumer::ConsumerGroupHeartbeatSuccess {
    success_with(1, 0)
}

pub(super) fn success_with(
    member_epoch: i32,
    partition: i32,
) -> crate::protocol::consumer::ConsumerGroupHeartbeatSuccess {
    consumer_group_heartbeat_success_for_test(member_epoch, partition)
}

pub(super) fn success_without_assignment(
    member_epoch: i32,
) -> crate::protocol::consumer::ConsumerGroupHeartbeatSuccess {
    consumer_group_heartbeat_success_without_assignment_for_test(member_epoch)
}
