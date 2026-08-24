//! Engine retry-gate, deadline, and rediscovery failure regressions.

use kafka_client_core::{
    Moment, ShareGroupHeartbeatFailure, ShareGroupHeartbeatPhase, ShareGroupHeartbeatRequestKind,
    ShareGroupHeartbeatRetryCause,
};

use super::super::{
    ShareMembershipFailureTurn, membership::ShareMembershipRetryGate, membership_test::begin,
    request_test::owner,
};

#[test]
fn rediscovery_requires_both_positive_delay_and_invalidation_permission() {
    let clock = crate::clock::MonotonicClock::new();
    let mut owner = owner();
    begin(&mut owner, &clock);
    let now = clock.now().unwrap_or_else(|error| panic!("now: {error:?}"));
    let rejected = owner
        .prepared()
        .unwrap_or_else(|| panic!("prepared"))
        .attempt;
    let turn = owner
        .settle_failure(now, &clock, ShareGroupHeartbeatFailure::Broker(16))
        .unwrap_or_else(|error| panic!("rediscovery: {error:?}"));
    let ShareMembershipFailureTurn::Rediscovery(schedule) = turn else {
        panic!("expected rediscovery")
    };
    assert_ne!(schedule.attempt(), rejected);
    assert_eq!(schedule.cause(), ShareGroupHeartbeatRetryCause::Rediscovery);
    assert!(!owner.is_ready_to_submit());
    assert_eq!(
        owner.prepare_request().err(),
        Some(super::super::ShareMembershipError::Occupied)
    );
    owner
        .permit_rediscovery()
        .unwrap_or_else(|error| panic!("permission: {error:?}"));
    assert!(!owner.is_ready_to_submit());
    assert_eq!(
        owner.prepare_request().err(),
        Some(super::super::ShareMembershipError::Occupied)
    );
    owner
        .observe_retry_due(schedule, Moment::from_tick(schedule.not_before().tick()))
        .unwrap_or_else(|error| panic!("retry due: {error:?}"));
    assert!(owner.is_ready_to_submit());
    assert!(owner.prepare_request().is_ok());
    assert_eq!(owner.retry_gate(), ShareMembershipRetryGate::Open);
}

#[test]
fn coordinator_load_retries_same_attempt_and_preserves_original_deadline() {
    let clock = crate::clock::MonotonicClock::new();
    let mut owner = owner();
    begin(&mut owner, &clock);
    let prepared = owner.prepared().unwrap_or_else(|| panic!("prepared"));
    let now = clock.now().unwrap_or_else(|error| panic!("now: {error:?}"));
    let turn = owner
        .settle_failure(now, &clock, ShareGroupHeartbeatFailure::Broker(14))
        .unwrap_or_else(|error| panic!("load retry: {error:?}"));
    let ShareMembershipFailureTurn::RetryScheduled(schedule) = turn else {
        panic!("expected retry")
    };
    assert_eq!(schedule.attempt(), prepared.attempt);
    assert_eq!(schedule.deadline(), prepared.deadline.core());
    owner
        .observe_retry_due(schedule, Moment::from_tick(schedule.not_before().tick()))
        .unwrap_or_else(|error| panic!("retry due: {error:?}"));
    assert!(owner.is_ready_to_submit());
}

#[test]
fn retry_due_at_original_deadline_terminalizes_and_clears_engine_owners() {
    let clock = crate::clock::MonotonicClock::new();
    let mut owner = owner();
    begin(&mut owner, &clock);
    let now = clock.now().unwrap_or_else(|error| panic!("now: {error:?}"));
    let turn = owner
        .settle_failure(now, &clock, ShareGroupHeartbeatFailure::Broker(14))
        .unwrap_or_else(|error| panic!("load retry: {error:?}"));
    let ShareMembershipFailureTurn::RetryScheduled(schedule) = turn else {
        panic!("expected retry")
    };
    let observed = owner
        .observe_retry_due(schedule, Moment::from_tick(schedule.deadline().tick()))
        .unwrap_or_else(|error| panic!("deadline terminal: {error:?}"));
    assert_eq!(
        observed,
        super::super::membership::ShareMembershipRetryDueTurn::Terminal
    );
    assert_eq!(owner.prepared(), None);
    assert_eq!(owner.retry_gate(), ShareMembershipRetryGate::Open);
    assert_eq!(owner.machine().phase(), ShareGroupHeartbeatPhase::Fatal);
    assert_eq!(
        owner.startup_failure(),
        Some(ShareGroupHeartbeatFailure::DeadlineElapsed)
    );
}

#[test]
fn failed_rediscovery_terminalizes_the_retained_startup_attempt() {
    let clock = crate::clock::MonotonicClock::new();
    let mut owner = owner();
    begin(&mut owner, &clock);
    let now = clock.now().unwrap_or_else(|error| panic!("now: {error:?}"));
    let turn = owner
        .settle_failure(now, &clock, ShareGroupHeartbeatFailure::Broker(16))
        .unwrap_or_else(|error| panic!("rediscovery: {error:?}"));
    assert!(matches!(turn, ShareMembershipFailureTurn::Rediscovery(_)));
    owner
        .fail_rediscovery(ShareGroupHeartbeatFailure::Execution)
        .unwrap_or_else(|error| panic!("failed rediscovery: {error:?}"));
    assert_eq!(owner.machine().phase(), ShareGroupHeartbeatPhase::Fatal);
    assert_eq!(
        owner.startup_failure(),
        Some(ShareGroupHeartbeatFailure::Execution)
    );
    assert_eq!(owner.prepared(), None);
    assert_eq!(owner.retry_gate(), ShareMembershipRetryGate::Open);
}

#[test]
fn expired_steady_route_revokes_assignment_and_gates_a_fresh_join() {
    let clock = crate::clock::MonotonicClock::new();
    let mut owner = owner();
    begin(&mut owner, &clock);
    let member = owner.catalog.member().to_owned();
    let now = clock.now().unwrap_or_else(|error| panic!("now: {error:?}"));
    owner
        .settle_success(
            now,
            crate::protocol::consumer::share_group::share_group_heartbeat_success_for_test(
                Some(&member),
                1,
                5_000,
                vec![([7; 16], vec![0])],
            ),
        )
        .unwrap_or_else(|error| panic!("membership success: {error:?}"));
    let schedule = owner
        .machine()
        .schedule()
        .unwrap_or_else(|| panic!("heartbeat schedule"));
    owner
        .prepare_heartbeat_due(Moment::from_tick(schedule.deadline().tick()), &clock)
        .unwrap_or_else(|error| panic!("steady due: {error:?}"));
    let rejected = owner.prepared().unwrap_or_else(|| panic!("steady attempt"));
    let turn = owner
        .settle_failure(
            Moment::from_tick(rejected.deadline.core().tick()),
            &clock,
            ShareGroupHeartbeatFailure::CoordinatorUnavailable,
        )
        .unwrap_or_else(|error| panic!("recover expired route: {error:?}"));
    let ShareMembershipFailureTurn::Rediscovery(retry) = turn else {
        panic!("rediscovery schedule")
    };
    let replacement = owner.prepared().unwrap_or_else(|| panic!("replacement"));
    assert_eq!(replacement.kind, ShareGroupHeartbeatRequestKind::Join);
    assert!(replacement.member_epoch.is_none());
    assert!(replacement.assignment_generation.is_none());
    assert!(replacement.deadline.core() > rejected.deadline.core());
    assert!(owner.activated_assignment().is_none());
    assert!(!owner.is_ready_to_submit());
    owner
        .permit_rediscovery()
        .unwrap_or_else(|error| panic!("route permission: {error:?}"));
    owner
        .observe_retry_due(retry, Moment::from_tick(retry.not_before().tick()))
        .unwrap_or_else(|error| panic!("retry due: {error:?}"));
    assert!(owner.is_ready_to_submit());
}
