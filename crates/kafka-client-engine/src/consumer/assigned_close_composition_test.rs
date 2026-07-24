//! Reservation-before-core composition evidence for assigned close.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, NextFetchOffset, PartitionIndex, StartPosition,
    TopicId,
};

use super::assigned_close_error::AssignedCloseSlotPhase;
use super::assigned_close_slot::AssignedCloseSlot;

#[test]
fn reservation_precedes_core_acceptance_and_ordered_cleanup() {
    let mut machine = AssignedConsumerMachine::new();
    let offset = NextFetchOffset::try_from_raw(0).unwrap_or_else(|| panic!("valid offset"));
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0)),
                StartPosition::Offset(offset),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("assign before close: {error}"));
    let mut slot = AssignedCloseSlot::create_for_assigned_owner();

    slot.reserve().unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Reserved);
    let transition = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("begin reserved close: {error}"));
    let Some(accept @ AssignedConsumerEffect::AcceptClose { close_id }) =
        transition.effects().first().copied()
    else {
        panic!("acceptance must precede assigned cleanup");
    };
    slot.observe_close_effect(accept)
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(slot.phase(), AssignedCloseSlotPhase::Accepted);
    assert_eq!(slot.accepted_id(), Ok(close_id));

    assert!(matches!(
        transition.effects().get(1),
        Some(AssignedConsumerEffect::Suspend { .. })
    ));
    assert!(matches!(
        transition.effects().get(2),
        Some(AssignedConsumerEffect::Revoke { .. })
    ));
}
