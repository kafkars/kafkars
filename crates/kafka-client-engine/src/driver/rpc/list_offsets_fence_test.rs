//! Control effects supersede only their exact retained position domain.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, PartitionIndex, PositionFence, StartPosition,
    TopicId,
};

use super::list_offsets_fence::supersedes;

#[test]
fn suspend_and_revoke_match_only_the_fenced_position_domain() {
    let (old, mut machine) = assignment();
    let seek = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: old.assignment_epoch(),
            partition: old.partition(),
            position: StartPosition::End,
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("seek: {error}"));
    let suspend = seek.effects()[0];
    let newer = resolve_fence(&seek);
    assert!(supersedes(suspend, old));
    assert!(!supersedes(
        AssignedConsumerEffect::Suspend { fence: old },
        newer
    ));

    let replacement = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                old.partition(),
                StartPosition::Beginning,
            )],
            now: Moment::from_tick(2),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("replacement: {error}"));
    let revoke = replacement.effects()[0];
    let replacement_fence = resolve_fence(&replacement);
    assert!(matches!(revoke, AssignedConsumerEffect::Revoke { .. }));
    assert!(supersedes(revoke, old));
    assert!(!supersedes(suspend, replacement_fence));
}

#[test]
fn older_or_different_assignment_suspend_never_stales_newer_work() {
    let (old, mut machine) = assignment();
    let seek = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: old.assignment_epoch(),
            partition: old.partition(),
            position: StartPosition::End,
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("seek: {error}"));
    let newer = resolve_fence(&seek);
    assert!(!supersedes(
        AssignedConsumerEffect::Suspend { fence: old },
        newer
    ));

    let replacement = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                old.partition(),
                StartPosition::Beginning,
            )],
            now: Moment::from_tick(2),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("replacement: {error}"));
    assert!(!supersedes(seek.effects()[0], resolve_fence(&replacement)));
}

fn assignment() -> (PositionFence, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(3)),
                StartPosition::Beginning,
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("assignment: {error}"));
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = transition.effects()[0] else {
        panic!("resolution effect");
    };
    (fence, machine)
}

fn resolve_fence(transition: &kafka_client_core::AssignedConsumerTransition) -> PositionFence {
    transition
        .effects()
        .iter()
        .find_map(|effect| match effect {
            AssignedConsumerEffect::ResolvePosition { fence, .. } => Some(*fence),
            _ => None,
        })
        .unwrap_or_else(|| panic!("resolution effect"))
}
