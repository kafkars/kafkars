//! Bounded ordering, replacement, fencing, and cancellation scenarios.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFence, FetchRecords, Moment, NextFetchOffset,
    PartitionIndex, PositionFence, StartPosition, TopicId,
};

use super::assigned_timer_model::{AssignedTimerDisposition, AssignedTimerError};
use super::assigned_timers::AssignedTimers;

#[test]
fn suspend_cancels_the_exact_timer_and_fences_stale_rearm() {
    let (old_effect, mut machine) = position_timer(1, 3, 15);
    let old_fence = position_fence(old_effect);
    let mut timers = AssignedTimers::new(1);

    assert_eq!(
        arm(&mut timers, old_effect),
        Ok(AssignedTimerDisposition::Inserted)
    );
    assert_eq!(timers.next_deadline(), Some(Deadline::from_tick(15)));

    let pause = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: old_fence.assignment_epoch(),
            partition: old_fence.partition(),
        })
        .unwrap_or_else(|error| panic!("pause throttled position: {error}"));
    let [suspend @ AssignedConsumerEffect::Suspend { .. }] = pause.effects() else {
        panic!("pause must emit one suspension fence");
    };
    assert!(timers.observe_control(*suspend));
    assert_eq!(timers.timer_count(), 0);
    assert_eq!(timers.next_deadline(), None);

    let resume = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: old_fence.assignment_epoch(),
            partition: old_fence.partition(),
            now: Moment::from_tick(12),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("resume throttled position: {error}"));
    let [new_effect @ AssignedConsumerEffect::ArmPositionThrottle { .. }] = resume.effects() else {
        panic!("resume must rearm the retained deadline");
    };
    let new_fence = position_fence(*new_effect);
    assert_eq!(
        arm(&mut timers, *new_effect),
        Ok(AssignedTimerDisposition::Inserted)
    );
    assert_eq!(timers.next_deadline(), Some(Deadline::from_tick(15)));
    assert_eq!(
        timers.arm_position(old_fence, Deadline::from_tick(1)),
        Ok(AssignedTimerDisposition::Fenced)
    );
    assert_eq!(
        timers.pop_due(Moment::from_tick(15)),
        Some(AssignedConsumerInput::PositionThrottleElapsed {
            fence: new_fence,
            now: Moment::from_tick(15),
        })
    );
}

pub(super) fn arm(
    timers: &mut AssignedTimers,
    effect: AssignedConsumerEffect,
) -> Result<AssignedTimerDisposition, AssignedTimerError> {
    match effect {
        AssignedConsumerEffect::ArmPositionThrottle { fence, deadline } => {
            timers.arm_position(fence, deadline)
        }
        AssignedConsumerEffect::ArmFetchThrottle { fence, deadline } => {
            timers.arm_fetch(fence, deadline)
        }
        _ => panic!("test must provide a throttle timer"),
    }
}

pub(super) fn position_timer(
    topic: u64,
    partition: u32,
    deadline_tick: u64,
) -> (AssignedConsumerEffect, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let assignment = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(topic, partition, StartPosition::Beginning)],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("assign resolving position: {error}"));
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = assignment.effects()[0] else {
        panic!("beginning position must resolve");
    };
    let transition = machine
        .apply(AssignedConsumerInput::PositionResolved {
            fence,
            next_offset: offset(8),
            now: Moment::from_tick(5),
            throttle_ticks: deadline_tick - 5,
        })
        .unwrap_or_else(|error| panic!("resolve throttled position: {error}"));
    (transition.effects()[0], machine)
}

pub(super) fn fetch_timer(
    topic: u64,
    partition: u32,
    deadline_tick: u64,
) -> (AssignedConsumerEffect, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let assignment = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(topic, partition, StartPosition::Offset(offset(7)))],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("assign Fetch position: {error}"));
    let AssignedConsumerEffect::FetchReady { fence, .. } = assignment.effects()[0] else {
        panic!("explicit offset must fetch");
    };
    let transition = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(8),
            now: Moment::from_tick(5),
            throttle_ticks: deadline_tick - 5,
        })
        .unwrap_or_else(|error| panic!("advance throttled Fetch: {error}"));
    (transition.effects()[0], machine)
}

pub(super) fn assigned(topic: u64, partition: u32, start: StartPosition) -> AssignedPartition {
    AssignedPartition::new(
        AssignedTopicPartition::new(
            TopicId::from_raw(topic),
            PartitionIndex::from_raw(partition),
        ),
        start,
    )
}

pub(super) fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("test offset is nonnegative"))
}

pub(super) fn position_fence(effect: AssignedConsumerEffect) -> PositionFence {
    let AssignedConsumerEffect::ArmPositionThrottle { fence, .. } = effect else {
        panic!("position timer effect");
    };
    fence
}

pub(super) fn fetch_fence(effect: AssignedConsumerEffect) -> FetchFence {
    let AssignedConsumerEffect::ArmFetchThrottle { fence, .. } = effect else {
        panic!("Fetch timer effect");
    };
    fence
}
