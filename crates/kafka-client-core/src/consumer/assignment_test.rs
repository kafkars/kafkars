//! Direct-assignment replacement and ordered-effect scenarios.

use crate::{Deadline, Moment, PartitionIndex, TopicId};

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, AssignedPartition, AssignedTopicPartition, AssignmentEpoch,
    NextFetchOffset, StartPosition,
};

#[test]
fn explicit_start_positions_emit_in_assignment_order() {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![
                assigned(2, 0, StartPosition::Beginning),
                assigned(1, 4, StartPosition::Offset(offset(9))),
                assigned(2, 3, StartPosition::End),
            ],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("initial direct assignment: {error}"));

    assert_eq!(transition.assignment_epoch().get(), 1);
    let effects = transition.effects();
    assert_eq!(effects.len(), 3);
    assert!(matches!(
        effects[0],
        AssignedConsumerEffect::ResolvePosition {
            fence,
            position: StartPosition::Beginning,
            ..
        } if fence.partition() == partition(2, 0)
    ));
    assert!(matches!(
        effects[1],
        AssignedConsumerEffect::FetchReady {
            fence,
            next_offset,
        } if fence.position().partition() == partition(1, 4)
            && next_offset == offset(9)
    ));
    assert!(matches!(
        effects[2],
        AssignedConsumerEffect::ResolvePosition {
            fence,
            position: StartPosition::End,
            ..
        } if fence.partition() == partition(2, 3)
    ));
}

#[test]
fn replacement_revokes_old_order_before_starting_new_epoch() {
    let mut machine = AssignedConsumerMachine::new();
    assign(
        &mut machine,
        vec![
            assigned(1, 3, StartPosition::Offset(offset(1))),
            assigned(1, 1, StartPosition::Beginning),
        ],
    );
    let replacement = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(4, 2, StartPosition::End)],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("replacement assignment: {error}"));

    assert_eq!(replacement.assignment_epoch().get(), 2);
    assert_eq!(replacement.effects().len(), 3);
    assert!(matches!(
        replacement.effects()[0],
        AssignedConsumerEffect::Revoke {
            assignment_epoch,
            partition: revoked,
        } if assignment_epoch.get() == 1 && revoked == partition(1, 3)
    ));
    assert!(matches!(
        replacement.effects()[1],
        AssignedConsumerEffect::Revoke {
            assignment_epoch,
            partition: revoked,
        } if assignment_epoch.get() == 1 && revoked == partition(1, 1)
    ));
    assert!(matches!(
        replacement.effects()[2],
        AssignedConsumerEffect::ResolvePosition {
            fence,
            position: StartPosition::End,
            ..
        } if fence.assignment_epoch().get() == 2
            && fence.partition() == partition(4, 2)
    ));
}

#[test]
fn invalid_assignment_does_not_consume_the_first_epoch() {
    let duplicate = assigned(1, 2, StartPosition::Beginning);
    let mut machine = AssignedConsumerMachine::new();
    assert_eq!(
        machine.apply(AssignedConsumerInput::Assign {
            partitions: vec![duplicate, duplicate],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        }),
        Err(AssignedConsumerMachineError::DuplicatePartition {
            partition: partition(1, 2),
        })
    );
    assert_eq!(machine.assignment_epoch(), None);

    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(1, 2, StartPosition::Beginning)],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("valid assignment after rejection: {error}"));
    assert_eq!(transition.assignment_epoch(), AssignmentEpoch::initial());
}

pub(super) fn assign(
    machine: &mut AssignedConsumerMachine,
    partitions: Vec<AssignedPartition>,
) -> super::AssignedConsumerTransition {
    assign_at(
        machine,
        partitions,
        Moment::from_tick(0),
        Deadline::from_tick(100),
    )
}

pub(super) fn assign_at(
    machine: &mut AssignedConsumerMachine,
    partitions: Vec<AssignedPartition>,
    now: Moment,
    resolution_deadline: Deadline,
) -> super::AssignedConsumerTransition {
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions,
            now,
            resolution_deadline,
        })
        .unwrap_or_else(|error| panic!("test assignment: {error}"))
}

pub(super) const fn assigned(
    topic: u64,
    raw_partition: u32,
    start: StartPosition,
) -> AssignedPartition {
    AssignedPartition::new(partition(topic, raw_partition), start)
}

pub(super) const fn partition(topic: u64, raw_partition: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(raw_partition),
    )
}

pub(super) fn offset(raw: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(raw).unwrap_or_else(|| panic!("nonnegative test offset"))
}
