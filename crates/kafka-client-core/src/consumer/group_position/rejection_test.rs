//! Exact group and partition broker rejection precedence scenarios.

use core::num::NonZeroI16;

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, MemberId, MembershipCycle,
    Moment, PartitionIndex, TopicId,
};

use super::{
    GroupPositionBatch, GroupPositionBootstrapEffect, GroupPositionBootstrapFailureKind,
    GroupPositionBootstrapInput, GroupPositionBootstrapMachine, GroupPositionBootstrapTerminal,
    GroupPositionBrokerError, GroupPositionFence, GroupPositionPartitionFact,
    GroupPositionPartitionResult,
};

#[test]
fn group_and_partition_rejections_preserve_exact_signed_codes() {
    let fence = position_fence();
    let group_error = GroupPositionBrokerError::new(nonzero(-32_001));
    let mut group_machine = submitted_machine(vec![assigned(3, 0)]);
    let transition = group_machine
        .apply(GroupPositionBootstrapInput::BrokerRejected {
            fence,
            now: Moment::from_tick(9),
            error: group_error,
        })
        .unwrap_or_else(|error| panic!("group rejection: {error}"));
    assert!(matches!(
        transition.into_effect(),
        Some(GroupPositionBootstrapEffect::Complete {
            terminal: GroupPositionBootstrapTerminal::Failed(failure),
            ..
        }) if failure.kind() == GroupPositionBootstrapFailureKind::Broker(group_error)
    ));

    let partition_error = GroupPositionBrokerError::new(nonzero(73));
    let mut partition_machine = submitted_machine(vec![assigned(3, 0)]);
    let transition = partition_machine
        .apply(GroupPositionBootstrapInput::OffsetsFetched {
            fence,
            now: Moment::from_tick(9),
            batch: GroupPositionBatch::new(
                29,
                vec![GroupPositionPartitionFact::rejected(
                    assigned(3, 0),
                    partition_error,
                )],
            ),
        })
        .unwrap_or_else(|error| panic!("partition rejection: {error}"));
    let Some(GroupPositionBootstrapEffect::Complete {
        terminal: GroupPositionBootstrapTerminal::PartitionRejected(rejection),
        ..
    }) = transition.into_effect()
    else {
        panic!("partition error must be a correlated rejection");
    };
    let GroupPositionPartitionResult::Rejected(actual) = rejection.first_rejected().result() else {
        panic!("exact partition rejection must survive");
    };
    assert_eq!(actual.code(), 73);
    assert_eq!(rejection.batch().throttle_time_ms(), 29);
}

#[test]
fn partition_rejection_dominates_missing_and_preserves_the_full_ordered_batch() {
    let fence = position_fence();
    let mut machine = submitted_machine(vec![assigned(3, 0), assigned(3, 2)]);
    let error = GroupPositionBrokerError::new(nonzero(73));
    let transition = machine
        .apply(GroupPositionBootstrapInput::OffsetsFetched {
            fence,
            now: Moment::from_tick(9),
            batch: GroupPositionBatch::new(
                31,
                vec![
                    GroupPositionPartitionFact::missing(assigned(3, 0)),
                    GroupPositionPartitionFact::rejected(assigned(3, 2), error),
                ],
            ),
        })
        .unwrap_or_else(|apply_error| panic!("mixed response: {apply_error}"));
    let Some(GroupPositionBootstrapEffect::Complete {
        terminal: GroupPositionBootstrapTerminal::PartitionRejected(rejection),
        ..
    }) = transition.into_effect()
    else {
        panic!("partition rejection must dominate missing-offset policy");
    };

    assert_eq!(rejection.batch().throttle_time_ms(), 31);
    assert_eq!(rejection.batch().facts().len(), 2);
    assert_eq!(
        rejection.batch().facts()[0].result(),
        GroupPositionPartitionResult::Missing
    );
    assert_eq!(rejection.first_rejected().partition(), assigned(3, 2));
}

fn submitted_machine(partitions: Vec<GroupAssignmentPartition>) -> GroupPositionBootstrapMachine {
    let fence = position_fence();
    let mut machine =
        GroupPositionBootstrapMachine::try_new(fence, Deadline::from_tick(20), partitions)
            .unwrap_or_else(|error| panic!("valid machine: {error}"));
    machine
        .apply(GroupPositionBootstrapInput::Start {
            fence,
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(GroupPositionBootstrapInput::DriverAccepted { fence }))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn position_fence() -> GroupPositionFence {
    GroupPositionFence::new(
        GroupId::try_from_raw(7).unwrap_or_else(|| panic!("group")),
        MembershipCycle::try_from_raw(11).unwrap_or_else(|| panic!("cycle")),
        MemberId::try_from_raw(13).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(2).unwrap_or_else(|| panic!("generation")),
    )
}

fn assigned(topic: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

fn nonzero(value: i16) -> NonZeroI16 {
    NonZeroI16::new(value).unwrap_or_else(|| panic!("nonzero broker code"))
}
