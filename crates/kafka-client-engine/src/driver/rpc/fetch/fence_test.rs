//! Pause, seek, and reassignment fence only directionally older Fetch calls.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFence, Moment, NextFetchOffset, PartitionIndex,
    StartPosition, TopicId,
};

use super::fence::supersedes;

#[test]
fn seek_suspend_supersedes_only_the_older_same_partition_fetch() {
    let (old, mut machine) = assignment(&[3]);
    let transition = seek(&mut machine, old[0], 52);
    let suspend = transition.effects()[0];
    let newer = fetch_fence(&transition);

    assert!(supersedes(suspend, old[0]));
    assert!(!supersedes(
        AssignedConsumerEffect::Suspend {
            fence: old[0].position(),
        },
        newer,
    ));
}

#[test]
fn control_for_another_partition_never_fences_the_candidate() {
    let (fences, mut machine) = assignment(&[3, 4]);
    let transition = seek(&mut machine, fences[0], 52);

    assert!(!supersedes(transition.effects()[0], fences[1]));
}

#[test]
fn reassignment_revoke_matches_only_the_old_assignment_domain() {
    let (old, mut machine) = assignment(&[3]);
    let replacement = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![partition(3, 52)],
            now: Moment::from_tick(2),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("replacement assignment: {error}"));
    let revoke = replacement.effects()[0];
    let newer = fetch_fence(&replacement);

    assert!(supersedes(revoke, old[0]));
    assert!(!supersedes(revoke, newer));
}

fn assignment(partitions: &[u32]) -> (Vec<FetchFence>, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let assigned = partitions
        .iter()
        .map(|index| partition(*index, 42))
        .collect();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: assigned,
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    let fences = transition
        .effects()
        .iter()
        .filter_map(|effect| match effect {
            AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
            _ => None,
        })
        .collect();
    (fences, machine)
}

fn seek(
    machine: &mut AssignedConsumerMachine,
    old: FetchFence,
    offset: i64,
) -> kafka_client_core::AssignedConsumerTransition {
    machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: old.position().assignment_epoch(),
            partition: old.position().partition(),
            position: StartPosition::Offset(
                NextFetchOffset::try_from_raw(offset)
                    .unwrap_or_else(|| panic!("valid replacement offset")),
            ),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("seek: {error}"))
}

fn fetch_fence(transition: &kafka_client_core::AssignedConsumerTransition) -> FetchFence {
    transition
        .effects()
        .iter()
        .find_map(|effect| match effect {
            AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
            _ => None,
        })
        .unwrap_or_else(|| panic!("FetchReady effect"))
}

fn partition(index: u32, offset: i64) -> AssignedPartition {
    AssignedPartition::new(
        AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(index)),
        StartPosition::Offset(
            NextFetchOffset::try_from_raw(offset).unwrap_or_else(|| panic!("valid offset")),
        ),
    )
}
