//! Exhaustive semantic seek-terminal translation tests.

use core::num::NonZeroI16;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, PartitionIndex, PositionFence,
    PositionResolutionAttemptFailure, PositionResolutionFailure, StartPosition, TopicId,
};

use crate::consumer::group_seek::{
    GroupConsumerSeekTerminal, GroupConsumerSeekTerminalFailure,
    GroupConsumerSeekTerminalFailureKind,
};

use super::seek_terminal::seek_terminal;

#[test]
fn throttle_success_and_signed_broker_failure_are_terminal() {
    let fence = position_fence();
    assert_eq!(
        seek_terminal(
            fence,
            AssignedConsumerEffect::ArmPositionThrottle {
                fence,
                deadline: Deadline::from_tick(91),
            },
        ),
        Some(GroupConsumerSeekTerminal::Succeeded)
    );
    let code = NonZeroI16::new(-731).unwrap_or_else(|| panic!("broker code"));
    assert_eq!(
        seek_terminal(
            fence,
            AssignedConsumerEffect::PositionResolutionFailed {
                fence,
                failure: PositionResolutionFailure::Attempt(
                    PositionResolutionAttemptFailure::Broker(code),
                ),
            },
        ),
        Some(GroupConsumerSeekTerminal::Failed(
            GroupConsumerSeekTerminalFailure {
                kind: GroupConsumerSeekTerminalFailureKind::BrokerRejected,
                broker_code: Some(-731),
            }
        ))
    );
}

fn position_fence() -> PositionFence {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0)),
                StartPosition::Beginning,
            )],
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(91),
        })
        .unwrap_or_else(|error| panic!("assignment: {error}"));
    let [AssignedConsumerEffect::ResolvePosition { fence, .. }] = transition.effects() else {
        panic!("beginning assignment resolves one position");
    };
    *fence
}
