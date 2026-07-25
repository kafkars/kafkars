//! Classic-group timeout validation and immutable emission evidence.

use crate::{Deadline, GroupId, Moment};

use super::{
    CLASSIC_GROUP_TIMEOUT_MAX_MS, CLASSIC_GROUP_TIMEOUT_MIN_MS, ClassicGroupEffect,
    ClassicGroupInput, ClassicGroupMachine, ClassicGroupTiming, ClassicGroupTimingError,
    ClassicHeartbeatPolicy,
};

#[test]
fn exact_positive_signed_wire_domain_is_accepted() {
    let minimum = timing(CLASSIC_GROUP_TIMEOUT_MIN_MS, CLASSIC_GROUP_TIMEOUT_MIN_MS);
    assert_eq!(minimum.session_timeout_ms(), 1);
    assert_eq!(minimum.rebalance_timeout_ms(), 1);

    let maximum = timing(CLASSIC_GROUP_TIMEOUT_MAX_MS, CLASSIC_GROUP_TIMEOUT_MAX_MS);
    assert_eq!(maximum.session_timeout_ms(), i32::MAX);
    assert_eq!(maximum.rebalance_timeout_ms(), i32::MAX);
}

#[test]
fn each_timeout_rejects_zero_and_one_past_the_wire_domain() {
    assert_eq!(
        ClassicGroupTiming::try_new(0, 1),
        Err(ClassicGroupTimingError::SessionTimeout { actual_ms: 0 })
    );
    assert_eq!(
        ClassicGroupTiming::try_new(CLASSIC_GROUP_TIMEOUT_MAX_MS + 1, 1),
        Err(ClassicGroupTimingError::SessionTimeout {
            actual_ms: CLASSIC_GROUP_TIMEOUT_MAX_MS + 1,
        })
    );
    assert_eq!(
        ClassicGroupTiming::try_new(1, 0),
        Err(ClassicGroupTimingError::RebalanceTimeout { actual_ms: 0 })
    );
    assert_eq!(
        ClassicGroupTiming::try_new(1, CLASSIC_GROUP_TIMEOUT_MAX_MS + 1),
        Err(ClassicGroupTimingError::RebalanceTimeout {
            actual_ms: CLASSIC_GROUP_TIMEOUT_MAX_MS + 1,
        })
    );
}

#[test]
fn independent_protocol_fields_have_no_processing_lease_ordering() {
    let shorter_rebalance = timing(30_000, 10_000);
    assert_eq!(shorter_rebalance.session_timeout_ms(), 30_000);
    assert_eq!(shorter_rebalance.rebalance_timeout_ms(), 10_000);

    let longer_rebalance = timing(10_000, 30_000);
    assert_eq!(longer_rebalance.session_timeout_ms(), 10_000);
    assert_eq!(longer_rebalance.rebalance_timeout_ms(), 30_000);
}

#[test]
fn session_timeout_has_one_exact_nanosecond_tick_conversion() {
    assert_eq!(timing(1, 1).session_timeout_ticks(), 1_000_000);
    assert_eq!(
        timing(CLASSIC_GROUP_TIMEOUT_MAX_MS, 1).session_timeout_ticks(),
        CLASSIC_GROUP_TIMEOUT_MAX_MS * 1_000_000
    );
}

#[test]
fn machine_retains_and_emits_the_exact_timing_on_every_join_cycle() {
    let expected = timing(12_345, 54_321);
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group"));
    let heartbeat = ClassicHeartbeatPolicy::try_new(10, 20)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"));
    let rejoin = super::ClassicRejoinPolicy::try_new(5, 50)
        .unwrap_or_else(|error| panic!("valid rejoin policy: {error:?}"));
    let mut machine = ClassicGroupMachine::new(group_id, expected, heartbeat, rejoin);

    assert_eq!(machine.timing(), expected);
    let transition = machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("valid begin: {error}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicGroupEffect::Join { timing, .. }) if *timing == expected
    ));

    let cycle = machine
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle"));
    machine
        .apply(ClassicGroupInput::JoinFailed { cycle })
        .unwrap_or_else(|error| panic!("valid Join failure: {error}"));
    let next = machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(2),
            deadline: Deadline::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("valid second begin: {error}"));
    assert!(matches!(
        next.effects().next(),
        Some(ClassicGroupEffect::Join { timing, .. }) if *timing == expected
    ));
}

fn timing(session_timeout_ms: u64, rebalance_timeout_ms: u64) -> ClassicGroupTiming {
    match ClassicGroupTiming::try_new(session_timeout_ms, rebalance_timeout_ms) {
        Ok(timing) => timing,
        Err(error) => panic!("valid classic group timing: {error}"),
    }
}
