//! Exhaustive classic input dispatch evidence.

use crate::{Deadline, GroupId, Moment};

use super::{
    ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupInput, ClassicGroupMachine,
    ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatPolicy,
};

#[test]
fn begin_dispatches_to_the_join_transition() {
    let group = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group"));
    let mut machine =
        ClassicGroupMachine::new(group, timing(), heartbeat_policy(), rejoin_policy());
    let transition = machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(9),
        })
        .unwrap_or_else(|error| panic!("valid begin: {error}"));

    assert_eq!(machine.phase(), ClassicGroupPhase::Joining);
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Join { .. })
    ));
}

#[test]
fn elapsed_start_emits_no_join() {
    let group = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group"));
    let mut machine =
        ClassicGroupMachine::new(group, timing(), heartbeat_policy(), rejoin_policy());
    let error = machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(10),
            deadline: Deadline::from_tick(10),
        })
        .err()
        .unwrap_or_else(|| panic!("elapsed begin must reject"));

    assert_eq!(error.kind(), ClassicGroupErrorKind::DeadlineElapsed);
    assert_eq!(machine.phase(), ClassicGroupPhase::Dormant);
}

fn timing() -> ClassicGroupTiming {
    ClassicGroupTiming::try_new(10_000, 30_000)
        .unwrap_or_else(|error| panic!("valid timing: {error}"))
}

fn heartbeat_policy() -> ClassicHeartbeatPolicy {
    ClassicHeartbeatPolicy::try_new(10, 20)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"))
}

fn rejoin_policy() -> super::ClassicRejoinPolicy {
    super::ClassicRejoinPolicy::try_new(5, 50)
        .unwrap_or_else(|error| panic!("valid rejoin policy: {error:?}"))
}
