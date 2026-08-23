//! Shared deterministic fixtures for share-membership tests.

use super::{
    ShareGroupHeartbeatAttempt, ShareGroupHeartbeatEffect, ShareGroupHeartbeatInput,
    ShareGroupHeartbeatMachine, ShareGroupHeartbeatPolicy, ShareGroupHeartbeatTransition,
    ShareGroupMemberEpoch,
};
use crate::{
    Deadline, GroupAssignmentPartition, GroupId, MemberId, Moment, PartitionIndex, TopicId,
};

pub(super) fn joining() -> (ShareGroupHeartbeatMachine, ShareGroupHeartbeatAttempt) {
    let mut machine = machine();
    let transition = machine
        .apply(ShareGroupHeartbeatInput::Begin {
            now: moment(10),
            deadline: deadline(40),
        })
        .unwrap_or_else(|error| panic!("begin share heartbeat: {error}"));
    let Some(ShareGroupHeartbeatEffect::Submit { attempt, .. }) = transition.into_effects().next()
    else {
        panic!("join attempt")
    };
    (machine, attempt)
}

pub(super) fn heartbeating() -> (ShareGroupHeartbeatMachine, ShareGroupHeartbeatAttempt) {
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
    let schedule = machine.schedule().unwrap_or_else(|| panic!("schedule"));
    let transition = machine
        .apply(ShareGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(schedule.deadline().tick()),
        })
        .unwrap_or_else(|error| panic!("due share heartbeat: {error}"));
    let Some(ShareGroupHeartbeatEffect::Submit { attempt, .. }) = transition.into_effects().next()
    else {
        panic!("steady attempt")
    };
    (machine, attempt)
}

pub(super) fn succeed(
    machine: &mut ShareGroupHeartbeatMachine,
    attempt: ShareGroupHeartbeatAttempt,
    now: u64,
    member_epoch: i32,
    interval: u64,
    throttle: u64,
    assignment: Option<Vec<GroupAssignmentPartition>>,
) -> ShareGroupHeartbeatTransition {
    machine
        .apply(ShareGroupHeartbeatInput::HeartbeatSucceeded {
            attempt,
            now: moment(now),
            member_epoch: epoch(member_epoch),
            heartbeat_interval_ticks: interval,
            throttle_ticks: throttle,
            assignment,
        })
        .unwrap_or_else(|error| panic!("share heartbeat success: {error}"))
}

pub(super) fn partition(topic: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

pub(super) const fn moment(value: u64) -> Moment {
    Moment::from_tick(value)
}

pub(super) const fn deadline(value: u64) -> Deadline {
    Deadline::from_tick(value)
}

pub(super) fn group() -> GroupId {
    GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group id"))
}

pub(super) fn member() -> MemberId {
    MemberId::try_from_raw(9).unwrap_or_else(|| panic!("member id"))
}

pub(super) fn machine() -> ShareGroupHeartbeatMachine {
    ShareGroupHeartbeatMachine::new(
        group(),
        member(),
        ShareGroupHeartbeatPolicy::try_new(10).unwrap_or_else(|_| panic!("policy")),
    )
}

pub(super) fn epoch(value: i32) -> ShareGroupMemberEpoch {
    ShareGroupMemberEpoch::try_from_raw(value).unwrap_or_else(|| panic!("member epoch"))
}
