//! End-to-end engine interpretation of share membership and retry gates.

use std::time::Duration;

use super::request_test::owner;
use kafka_client_core::{
    Moment, ShareGroupHeartbeatFailure, ShareGroupHeartbeatPhase, ShareGroupHeartbeatRequestKind,
};

#[test]
fn assignment_is_installed_heartbeated_and_left_under_exact_fences() {
    let clock = crate::clock::MonotonicClock::new();
    let mut owner = owner();
    begin(&mut owner, &clock);
    owner
        .settle_success(
            clock.now().unwrap_or_else(|error| panic!("now: {error:?}")),
            success(1),
        )
        .unwrap_or_else(|error| panic!("settle: {error:?}"));
    assert_eq!(owner.machine().phase(), ShareGroupHeartbeatPhase::Stable);
    assert_eq!(
        owner
            .activated_assignment()
            .map(|assignment| assignment.partitions().len()),
        Some(2)
    );

    let schedule = owner
        .machine()
        .schedule()
        .unwrap_or_else(|| panic!("heartbeat schedule"));
    owner
        .prepare_heartbeat_due(Moment::from_tick(schedule.deadline().tick()), &clock)
        .unwrap_or_else(|error| panic!("due: {error:?}"));
    assert_eq!(
        owner.prepared().map(|prepared| prepared.kind),
        Some(ShareGroupHeartbeatRequestKind::Steady)
    );
    owner
        .settle_success(
            Moment::from_tick(schedule.deadline().tick().saturating_add(1)),
            success(1),
        )
        .unwrap_or_else(|error| panic!("steady: {error:?}"));

    let close = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("close capture: {error:?}"));
    owner
        .begin_leave(close)
        .unwrap_or_else(|error| panic!("leave: {error:?}"));
    owner
        .settle_leave_success()
        .unwrap_or_else(|error| panic!("leave terminal: {error:?}"));
    assert_eq!(owner.machine().phase(), ShareGroupHeartbeatPhase::Closed);
    assert!(owner.activated_assignment().is_none());
}

#[test]
fn replacement_assignment_advances_the_local_generation() {
    let clock = crate::clock::MonotonicClock::new();
    let mut owner = owner();
    begin(&mut owner, &clock);
    owner
        .settle_success(
            Moment::from_tick(1),
            success_with("stable-member", 1, &[0, 1], [7; 16]),
        )
        .unwrap_or_else(|error| panic!("initial assignment: {error:?}"));
    let initial = owner
        .activated_assignment()
        .unwrap_or_else(|| panic!("initial assignment"))
        .assignment_generation();
    let schedule = owner
        .machine()
        .schedule()
        .unwrap_or_else(|| panic!("schedule"));
    owner
        .prepare_heartbeat_due(Moment::from_tick(schedule.deadline().tick()), &clock)
        .unwrap_or_else(|error| panic!("heartbeat due: {error:?}"));
    owner
        .settle_success(
            Moment::from_tick(schedule.deadline().tick().saturating_add(1)),
            success_with("stable-member", 2, &[1], [7; 16]),
        )
        .unwrap_or_else(|error| panic!("replacement assignment: {error:?}"));
    let replacement = owner
        .activated_assignment()
        .unwrap_or_else(|| panic!("replacement assignment"));
    assert!(replacement.assignment_generation() > initial);
    assert_eq!(replacement.partitions().len(), 1);
}

#[test]
fn malformed_success_terminalizes_startup_without_installing_assignment() {
    let clock = crate::clock::MonotonicClock::new();
    let mut owner = owner();
    begin(&mut owner, &clock);
    owner
        .settle_success(
            Moment::from_tick(1),
            success_with("different-member", 1, &[0], [7; 16]),
        )
        .unwrap_or_else(|error| panic!("terminal malformed success: {error:?}"));
    assert_eq!(
        owner.startup_failure(),
        Some(ShareGroupHeartbeatFailure::InvalidResponse)
    );
    assert_eq!(owner.machine().phase(), ShareGroupHeartbeatPhase::Fatal);
    assert!(owner.activated_assignment().is_none());
}

#[test]
fn fenced_steady_member_revokes_before_fresh_epoch_zero_join() {
    let clock = crate::clock::MonotonicClock::new();
    let mut owner = owner();
    begin(&mut owner, &clock);
    owner
        .settle_success(Moment::from_tick(1), success(1))
        .unwrap_or_else(|error| panic!("initial assignment: {error:?}"));
    let schedule = owner
        .machine()
        .schedule()
        .unwrap_or_else(|| panic!("schedule"));
    owner
        .prepare_heartbeat_due(Moment::from_tick(schedule.deadline().tick()), &clock)
        .unwrap_or_else(|error| panic!("heartbeat due: {error:?}"));
    let rejected = owner.prepared().unwrap_or_else(|| panic!("steady attempt"));
    owner
        .settle_failure(
            Moment::from_tick(schedule.deadline().tick().saturating_add(1)),
            &clock,
            ShareGroupHeartbeatFailure::Broker(110),
        )
        .unwrap_or_else(|error| panic!("fenced recovery: {error:?}"));
    let prepared = owner.prepared().unwrap_or_else(|| panic!("fresh join"));
    assert_ne!(prepared.attempt, rejected.attempt);
    assert_eq!(prepared.kind, ShareGroupHeartbeatRequestKind::Join);
    assert_eq!(prepared.member_epoch, None);
    assert_eq!(prepared.assignment_generation, None);
    assert_eq!(owner.machine().phase(), ShareGroupHeartbeatPhase::Joining);
    assert!(owner.activated_assignment().is_none());
}

pub(super) fn begin(
    owner: &mut super::ShareMembershipInterpreter,
    clock: &crate::clock::MonotonicClock,
) {
    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    owner
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
}

fn success(epoch: i32) -> crate::protocol::consumer::share_group::ShareGroupHeartbeatSuccess {
    success_with("stable-member", epoch, &[0, 1], [7; 16])
}

fn success_with(
    member: &str,
    epoch: i32,
    partitions: &[i32],
    topic_id: [u8; 16],
) -> crate::protocol::consumer::share_group::ShareGroupHeartbeatSuccess {
    crate::protocol::consumer::share_group::share_group_heartbeat_success_for_test(
        Some(member),
        epoch,
        5_000,
        vec![(topic_id, partitions.to_vec())],
    )
}
