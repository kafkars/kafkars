//! Shared deterministic fixtures for KIP-848 heartbeat state-machine tests.

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPolicy, ConsumerGroupHeartbeatTransition,
    ConsumerGroupMemberEpoch,
};
use crate::{
    Deadline, GroupAssignmentPartition, GroupId, MemberId, Moment, PartitionIndex, TopicId,
};

pub(super) fn joining() -> (ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatAttempt) {
    let mut machine = machine();
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::Begin {
            now: moment(10),
            deadline: deadline(40),
        })
        .unwrap_or_else(|error| panic!("begin heartbeat: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) =
        transition.into_effects().next()
    else {
        panic!("join attempt")
    };
    (machine, attempt)
}

pub(super) fn heartbeating() -> (ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatAttempt) {
    let (mut machine, attempt) = joining();
    let _ = succeed(
        &mut machine,
        attempt,
        20,
        1,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );
    let schedule = machine
        .schedule()
        .unwrap_or_else(|| panic!("armed heartbeat"));
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(schedule.deadline().tick()),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) =
        transition.into_effects().next()
    else {
        panic!("steady heartbeat attempt")
    };
    (machine, attempt)
}

pub(super) fn staged_reconciliation() -> ConsumerGroupHeartbeatMachine {
    let (mut machine, attempt) = joining();
    let _ = succeed(
        &mut machine,
        attempt,
        20,
        1,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );
    let attempt = due_attempt(&mut machine);
    let transition = succeed(
        &mut machine,
        attempt,
        26,
        2,
        5,
        0,
        Some(vec![partition(1, 1)]),
    );
    let effects = transition.into_effects().collect::<Vec<_>>();
    assert!(matches!(
        effects.as_slice(),
        [ConsumerGroupHeartbeatEffect::Reconcile {
            previous: Some(previous),
            assignment,
            member_epoch,
            schedule,
        }] if previous.assignment_generation().get() == 1
            && assignment.assignment_generation().get() == 2
            && *member_epoch == epoch(2)
            && schedule.assignment_generation().get() == 1
    ));
    machine
}

pub(super) fn due_attempt(
    machine: &mut ConsumerGroupHeartbeatMachine,
) -> ConsumerGroupHeartbeatAttempt {
    let schedule = machine.schedule().unwrap_or_else(|| panic!("schedule"));
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(schedule.deadline().tick()),
        })
        .unwrap_or_else(|error| panic!("heartbeat due: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) =
        transition.into_effects().next()
    else {
        panic!("steady attempt")
    };
    attempt
}

pub(super) fn succeed(
    machine: &mut ConsumerGroupHeartbeatMachine,
    attempt: ConsumerGroupHeartbeatAttempt,
    now: u64,
    member_epoch: i32,
    interval: u64,
    throttle: u64,
    assignment: Option<Vec<GroupAssignmentPartition>>,
) -> ConsumerGroupHeartbeatTransition {
    machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatSucceeded {
            attempt,
            now: moment(now),
            member_id: member(9),
            member_epoch: epoch(member_epoch),
            heartbeat_interval_ticks: interval,
            throttle_ticks: throttle,
            assignment,
        })
        .unwrap_or_else(|error| panic!("heartbeat success: {error}"))
}

pub(super) fn partition(topic: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

pub(super) fn member(value: u64) -> MemberId {
    MemberId::try_from_raw(value).unwrap_or_else(|| panic!("member id"))
}

pub(super) const fn moment(value: u64) -> Moment {
    Moment::from_tick(value)
}

pub(super) const fn deadline(value: u64) -> Deadline {
    Deadline::from_tick(value)
}

pub(super) fn machine() -> ConsumerGroupHeartbeatMachine {
    ConsumerGroupHeartbeatMachine::new(
        GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group id")),
        ConsumerGroupHeartbeatPolicy::try_new(10).unwrap_or_else(|_| panic!("policy")),
    )
}

pub(super) fn epoch(value: i32) -> ConsumerGroupMemberEpoch {
    ConsumerGroupMemberEpoch::try_from_raw(value).unwrap_or_else(|| panic!("member epoch"))
}
