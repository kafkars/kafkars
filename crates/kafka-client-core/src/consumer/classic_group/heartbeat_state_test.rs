//! Conservative session-liveness anchoring evidence for classic heartbeats.

use crate::{AssignmentGeneration, Deadline, GroupId, MemberId, Moment};

use super::{
    ClassicGeneration, ClassicGroupInput, ClassicGroupMachine, ClassicGroupPhase,
    ClassicGroupTiming, ClassicHeartbeatPolicy, MembershipCycle,
    heartbeat_state::{ClassicHeartbeatDue, ClassicHeartbeatState, ClassicHeartbeatSuccess},
};

#[test]
fn attempt_deadline_is_clamped_to_the_prior_confirmed_session_bound() {
    let mut state = state(2_000_000);
    let schedule = state
        .prepare_activation(
            MembershipCycle::initial(),
            assignment_generation(),
            Moment::from_tick(3),
            Deadline::from_tick(900_000),
        )
        .unwrap_or_else(|error| panic!("valid activation: {error:?}"))
        .unwrap_or_else(|| panic!("live activation"));
    let attempt = schedule.attempt();
    state.activate(schedule);

    assert!(matches!(
        state.due(attempt, Moment::from_tick(3)),
        Ok(ClassicHeartbeatDue::Submit(deadline))
            if deadline == Deadline::from_tick(900_000)
    ));
}

#[test]
fn response_latency_or_throttle_cannot_extend_submission_anchored_liveness() {
    let mut late = inflight_at(20, 99, 100);
    let late_attempt = attempt();
    assert!(matches!(
        late.succeeded(late_attempt, Moment::from_tick(100), 0, 1_000_000),
        Ok(ClassicHeartbeatSuccess::Lost)
    ));

    let mut throttled = inflight(2_000_000);
    let throttled_attempt = attempt();
    assert!(matches!(
        throttled.succeeded(
            throttled_attempt,
            Moment::from_tick(4),
            1_000_000,
            1_000_000
        ),
        Ok(ClassicHeartbeatSuccess::Lost)
    ));
}

#[test]
fn session_horizon_overflow_loses_the_assignment_instead_of_stalling_inflight() {
    let sent_at = u64::MAX - 1;
    let mut state = inflight_at(20, sent_at, u64::MAX);
    let attempt = attempt();

    assert!(matches!(
        state.succeeded(attempt, Moment::from_tick(sent_at), 0, 2),
        Ok(ClassicHeartbeatSuccess::Lost)
    ));
    assert!(matches!(
        state.succeeded(attempt, Moment::from_tick(sent_at), 0, 1),
        Err(super::ClassicGroupErrorKind::InvalidPhase)
    ));
}

#[test]
fn sync_after_join_anchored_liveness_loses_without_partial_install() {
    let mut machine = ClassicGroupMachine::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group")),
        ClassicGroupTiming::try_new(1, 30_000)
            .unwrap_or_else(|error| panic!("valid timing: {error}")),
        policy(10, 20),
        super::ClassicRejoinPolicy::try_new(5, 50)
            .unwrap_or_else(|error| panic!("valid rejoin policy: {error:?}")),
    );
    machine
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(2_000_000),
        })
        .unwrap_or_else(|error| panic!("valid Begin: {error}"));
    let cycle = machine
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle"));
    machine
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id: MemberId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero member")),
            generation: ClassicGeneration::try_from_raw(1)
                .unwrap_or_else(|| panic!("nonnegative generation")),
        })
        .unwrap_or_else(|error| panic!("valid Join: {error}"));
    let transition = machine
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(1_000_002),
            partitions: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("expired Sync is deterministic: {error}"));

    assert!(transition.effects().next().is_none());
    assert_eq!(machine.phase(), ClassicGroupPhase::Lost);
    assert_eq!(machine.live_assignment(), None);
}

fn inflight(attempt_timeout_ticks: u64) -> ClassicHeartbeatState {
    inflight_at(attempt_timeout_ticks, 3, 900_000)
}

fn inflight_at(
    attempt_timeout_ticks: u64,
    sent_at: u64,
    liveness_deadline: u64,
) -> ClassicHeartbeatState {
    let mut state = state(attempt_timeout_ticks);
    let schedule = state
        .prepare_activation(
            MembershipCycle::initial(),
            assignment_generation(),
            Moment::from_tick(sent_at),
            Deadline::from_tick(liveness_deadline),
        )
        .unwrap_or_else(|error| panic!("valid activation: {error:?}"))
        .unwrap_or_else(|| panic!("live activation"));
    let attempt = schedule.attempt();
    state.activate(schedule);
    state
        .due(attempt, Moment::from_tick(sent_at))
        .unwrap_or_else(|error| panic!("valid due: {error:?}"));
    state
}

fn attempt() -> super::ClassicHeartbeatAttempt {
    super::ClassicHeartbeatAttempt::first(MembershipCycle::initial(), assignment_generation())
}

fn state(attempt_timeout_ticks: u64) -> ClassicHeartbeatState {
    ClassicHeartbeatState::new(policy(10, attempt_timeout_ticks))
}

fn policy(interval_ticks: u64, attempt_timeout_ticks: u64) -> ClassicHeartbeatPolicy {
    ClassicHeartbeatPolicy::try_new(interval_ticks, attempt_timeout_ticks)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"))
}

fn assignment_generation() -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(1).unwrap_or_else(|| panic!("nonzero assignment generation"))
}
