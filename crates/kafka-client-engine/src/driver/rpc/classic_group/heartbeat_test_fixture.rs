//! Real classic membership transitions that yield exact heartbeat attempt identities.

use kafka_client_core::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupInput, ClassicGroupMachine,
    ClassicGroupTiming, ClassicHeartbeatAttempt, ClassicHeartbeatPolicy, Deadline, GroupId,
    MemberId, Moment,
};

pub(super) fn heartbeat_attempts() -> (ClassicHeartbeatAttempt, ClassicHeartbeatAttempt) {
    let mut machine = machine();
    machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("valid Begin: {error}"));
    let cycle = machine
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle expected"));
    machine
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id: MemberId::try_from_raw(2)
                .unwrap_or_else(|| panic!("nonzero member expected")),
            generation: ClassicGeneration::try_from_raw(7)
                .unwrap_or_else(|| panic!("nonnegative generation expected")),
        })
        .unwrap_or_else(|error| panic!("valid follower Join: {error}"));
    let installed = machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(3),
            partitions: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("valid Sync success: {error}"));
    let (first, first_due) = installed
        .into_effects()
        .find_map(|effect| match effect {
            ClassicGroupEffect::Install { heartbeat, .. } => {
                Some((heartbeat.attempt(), heartbeat.due()))
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("Install heartbeat schedule expected"));
    machine
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt: first,
            now: Moment::from_tick(first_due.tick()),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    let succeeded = machine
        .apply(ClassicGroupInput::HeartbeatSucceeded {
            attempt: first,
            now: Moment::from_tick(first_due.tick() + 1),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("successful heartbeat: {error}"));
    let next = succeeded
        .into_effects()
        .find_map(|effect| match effect {
            ClassicGroupEffect::ArmHeartbeat { schedule } => Some(schedule.attempt()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("next heartbeat schedule expected"));
    (first, next)
}

fn machine() -> ClassicGroupMachine {
    ClassicGroupMachine::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group expected")),
        ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("valid timing: {error}")),
        ClassicHeartbeatPolicy::try_new(10, 20)
            .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}")),
    )
}
