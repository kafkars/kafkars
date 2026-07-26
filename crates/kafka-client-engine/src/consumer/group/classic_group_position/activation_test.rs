//! Position-to-Fetch fence, offset, throttle, and terminal-shape scenarios.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedTopicPartition, AssignmentGeneration,
    Deadline, GroupAssignmentPartition, GroupId, GroupPositionBatch, GroupPositionBootstrapEffect,
    GroupPositionBootstrapInput, GroupPositionBootstrapMachine, GroupPositionFence,
    GroupPositionPartitionFact, MemberId, MembershipCycle, Moment, NextFetchOffset, PartitionIndex,
    ResolvedAssignedPartition, TopicId,
};

use super::{
    ClassicGroupPositionActivationError, ClassicGroupPositionCompleted,
    prepare_classic_group_fetch_activation, test_support::completed_ready,
};

#[test]
fn ready_position_becomes_exact_resolved_input_at_terminal_observation() {
    let observed_at = Moment::from_tick(41);
    let completed = completed_ready(
        position_fence(7),
        observed_at,
        GroupPositionBatch::new(
            13,
            vec![
                committed(2, 1, 17),
                committed(2, 4, 23),
                committed(5, 0, 31),
            ],
        ),
    );

    let input = prepare_classic_group_fetch_activation(&completed, position_fence(7))
        .unwrap_or_else(|error| panic!("Fetch activation: {error:?}"));
    assert_eq!(input.expected_assignment_epoch(), None);
    assert_eq!(input.now(), observed_at);
    assert_eq!(input.throttle_ticks(), 13_000_000);
    assert_eq!(
        input.partitions(),
        &[resolved(2, 1, 17), resolved(2, 4, 23), resolved(5, 0, 31),]
    );

    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .install_resolved_assignment(input)
        .unwrap_or_else(|error| panic!("resolved install: {error}"));
    assert!(transition.effects().iter().all(|effect| {
        matches!(
            effect,
            AssignedConsumerEffect::ArmFetchThrottle { deadline, .. }
                if deadline.tick() == observed_at.tick() + 13_000_000
        )
    }));
}

#[test]
fn current_group_fence_is_required_before_any_activation_copy() {
    let completed = completed_ready(
        position_fence(7),
        Moment::from_tick(41),
        GroupPositionBatch::new(0, vec![committed(2, 1, 17)]),
    );
    assert_eq!(
        prepare_classic_group_fetch_activation(&completed, position_fence(8)).err(),
        Some(ClassicGroupPositionActivationError::FenceMismatch {
            completed: position_fence(7),
            current: position_fence(8),
        })
    );
    assert_eq!(completed.fence(), position_fence(7));
    assert_eq!(completed.observed_at(), Moment::from_tick(41));
}

#[test]
fn only_core_ready_terminals_can_cross_the_fetch_handoff() {
    let mut machine = GroupPositionBootstrapMachine::try_new(
        position_fence(7),
        Deadline::from_tick(50),
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("position machine: {error}"));
    let transition = machine
        .apply(GroupPositionBootstrapInput::Start {
            fence: position_fence(7),
            now: Moment::from_tick(50),
        })
        .unwrap_or_else(|error| panic!("position terminal: {error}"));
    let Some(GroupPositionBootstrapEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("deadline completion expected");
    };
    let completed = ClassicGroupPositionCompleted::new(machine, terminal, Moment::from_tick(50));

    assert_eq!(
        prepare_classic_group_fetch_activation(&completed, position_fence(7)).err(),
        Some(ClassicGroupPositionActivationError::TerminalNotReady)
    );
}

fn committed(topic: u64, partition: u32, offset: i64) -> GroupPositionPartitionFact {
    GroupPositionPartitionFact::committed(
        GroupAssignmentPartition::new(
            TopicId::from_raw(topic),
            PartitionIndex::from_raw(partition),
        ),
        NextFetchOffset::try_from_raw(offset).unwrap_or_else(|| panic!("next offset")),
    )
}

fn resolved(topic: u64, partition: u32, offset: i64) -> ResolvedAssignedPartition {
    ResolvedAssignedPartition::new(
        AssignedTopicPartition::new(
            TopicId::from_raw(topic),
            PartitionIndex::from_raw(partition),
        ),
        NextFetchOffset::try_from_raw(offset).unwrap_or_else(|| panic!("next offset")),
    )
}

fn position_fence(generation: u64) -> GroupPositionFence {
    GroupPositionFence::new(
        GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group")),
        MembershipCycle::initial(),
        MemberId::try_from_raw(5).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("assignment generation")),
    )
}
