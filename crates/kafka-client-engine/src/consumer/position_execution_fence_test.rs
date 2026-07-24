//! Directional stale-terminal ownership scenarios for position execution.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, PartitionIndex, PositionFence, StartPosition,
    TopicId,
};

use super::position_execution::{PositionExecutionError, PositionResolutionExecutor};

#[test]
fn newer_position_terminal_is_invariant_and_remains_owned() {
    let (active_fence, mut active) = assignment();
    let (source_fence, mut source) = assignment();
    let seek = source
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: source_fence.assignment_epoch(),
            partition: source_fence.partition(),
            position: StartPosition::End,
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("seek source position: {error}"));
    let newer = resolve_fence(&seek);
    let mut executor = PositionResolutionExecutor::new(1);
    executor.install_terminal_for_test(newer, Moment::from_tick(5));

    assert!(matches!(
        executor.poll(&mut active, Moment::from_tick(6)),
        Err(PositionExecutionError::Core(
            kafka_client_core::AssignedConsumerMachineError::StalePosition {
                active,
                supplied,
            }
        )) if active == active_fence && supplied == newer
    ));
    assert_eq!(executor.retained_count(), 1);
}

#[test]
fn future_assignment_terminal_is_invariant_and_remains_owned() {
    let (active_fence, mut active) = assignment();
    let (source_fence, mut source) = assignment();
    let replacement = source
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                source_fence.partition(),
                StartPosition::End,
            )],
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("replace source assignment: {error}"));
    let future = resolve_fence(&replacement);
    let mut executor = PositionResolutionExecutor::new(1);
    executor.install_terminal_for_test(future, Moment::from_tick(5));

    assert!(matches!(
        executor.poll(&mut active, Moment::from_tick(6)),
        Err(PositionExecutionError::Core(
            kafka_client_core::AssignedConsumerMachineError::StaleAssignment {
                active,
                supplied,
            }
        )) if active == active_fence.assignment_epoch()
            && supplied == future.assignment_epoch()
    ));
    assert_eq!(executor.retained_count(), 1);
}

#[test]
fn exact_fence_late_terminal_after_deadline_is_drained_and_released() {
    let (fence, mut machine) = assignment();
    machine
        .apply(AssignedConsumerInput::PositionResolutionDeadlineElapsed {
            fence,
            now: Moment::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("settle exact position deadline: {error}"));
    let mut executor = PositionResolutionExecutor::new(1);
    executor.install_terminal_for_test(fence, Moment::from_tick(21));

    assert!(matches!(
        executor.poll(&mut machine, Moment::from_tick(22)),
        Ok(None)
    ));
    assert_eq!(executor.retained_count(), 0);
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
            resolution_deadline: Deadline::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    (resolve_fence(&transition), machine)
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
