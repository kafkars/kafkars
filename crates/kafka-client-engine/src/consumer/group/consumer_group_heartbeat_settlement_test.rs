//! KIP-848 assignment installation into position, liveness, and catalog owners.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    GroupId, GroupPositionBatch, GroupPositionFence, GroupPositionPartitionFact,
    LiveGroupAssignment, Moment, NextFetchOffset,
};
use kafka_wire::{
    ConsumerGroupHeartbeatResponse,
    consumer_group_heartbeat_response::{Assignment, TopicPartitions},
};
use kafka_wire_core::Uuid;

use crate::{
    clock::MonotonicClock,
    config::{ValidatedConsumerFetchConfig, ValidatedConsumerLimits},
    driver::TopicPartitionCountFact,
    protocol::consumer::{
        ConsumerGroupHeartbeatOutcome, normalize_consumer_group_heartbeat_response,
    },
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
    registry_event::GroupConsumerStateError,
    registry_processing::GroupConsumerProcessingTurn,
    session_catalog::CurrentGroupSession,
};
use crate::consumer::{GroupConsumerMembershipEpoch, GroupConsumerProtocol};

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
            .and_then(|consumer| consumer.cycle())
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

#[test]
fn confirmed_static_modern_state_exposes_identity_without_sending_it_transactionally() {
    let (entry, _topic_id) = installed_modern_entry_with_instance(Some(&Arc::from("instance-a")));
    let group_id = entry.group_id();
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);

    let state = registry
        .group_state(group_id)
        .unwrap_or_else(|error| panic!("state observation: {error:?}"))
        .unwrap_or_else(|| panic!("confirmed state"));
    assert_eq!(
        state.metadata().membership_epoch(),
        GroupConsumerMembershipEpoch::Consumer { member_epoch: 1 }
    );
    assert_eq!(state.metadata().group_instance_id(), Some("instance-a"));
    assert_eq!(state.metadata().group_instance_id_arc(), None);
}

#[test]
fn group_state_rejects_consumer_protocol_with_only_a_classic_epoch() {
    let (mut entry, _topic_id) = installed_modern_entry();
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("installed modern assignment"));
    let classic_assignment = LiveGroupAssignment::try_new(
        assignment.group_id(),
        assignment.member_id(),
        assignment.assignment_generation(),
        assignment.partitions().to_vec(),
    )
    .unwrap_or_else(|error| panic!("matching classic assignment: {error}"));
    let member_id = assignment.member_id();
    let member = entry
        .catalog
        .current_member()
        .cloned()
        .unwrap_or_else(|| panic!("installed modern member"));
    let installed_cycle = entry
        .catalog
        .membership_cycle()
        .unwrap_or_else(|| panic!("installed modern cycle"));
    let modern_session = entry
        .catalog
        .consumer_current
        .take()
        .unwrap_or_else(|| panic!("installed modern session"));
    entry.catalog.current = Some(CurrentGroupSession {
        member_id,
        member,
        installed_cycle,
        classic_generation: 7,
        assignment: classic_assignment,
    });
    assert_eq!(entry.protocol, GroupConsumerProtocol::Consumer);
    assert_eq!(entry.catalog.classic_generation(), Some(7));
    assert_eq!(entry.catalog.consumer_group_member_epoch(), None);

    let group_id = entry.group_id();
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);
    assert_eq!(
        registry.group_state(group_id),
        Err(GroupConsumerStateError::EntryFault)
    );

    let entry = registry
        .entries
        .first_mut()
        .unwrap_or_else(|| panic!("modern entry"));
    entry.catalog.current = None;
    entry.catalog.consumer_current = Some(modern_session);
}

pub(super) fn installed_modern_entry() -> (GroupConsumerEntry, kafka_client_core::TopicId) {
    installed_modern_entry_with_instance(None)
}

fn installed_modern_entry_with_instance(
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

fn modern_entry_with_instance(group_instance_id: Option<&Arc<str>>) -> GroupConsumerEntry {
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
    let mut response = ConsumerGroupHeartbeatResponse::default();
    response.member_id = Some("member-a".into());
    response.member_epoch = member_epoch;
    response.heartbeat_interval_ms = 5_000;
    let mut topic = TopicPartitions::default();
    topic.topic_id = Uuid::from_bytes([7; 16]);
    topic.partitions = vec![partition];
    let mut assignment = Assignment::default();
    assignment.topic_partitions = vec![topic];
    response.assignment = Some(assignment);
    let outcome = normalize_consumer_group_heartbeat_response(0, &response)
        .unwrap_or_else(|error| panic!("normalize: {error:?}"));
    let ConsumerGroupHeartbeatOutcome::Succeeded(success) = outcome else {
        panic!("successful heartbeat")
    };
    success
}

pub(super) fn success_without_assignment(
    member_epoch: i32,
) -> crate::protocol::consumer::ConsumerGroupHeartbeatSuccess {
    let mut response = ConsumerGroupHeartbeatResponse::default();
    response.member_id = Some("member-a".into());
    response.member_epoch = member_epoch;
    response.heartbeat_interval_ms = 5_000;
    let outcome = normalize_consumer_group_heartbeat_response(0, &response)
        .unwrap_or_else(|error| panic!("normalize: {error:?}"));
    let ConsumerGroupHeartbeatOutcome::Succeeded(success) = outcome else {
        panic!("successful steady heartbeat")
    };
    success
}
