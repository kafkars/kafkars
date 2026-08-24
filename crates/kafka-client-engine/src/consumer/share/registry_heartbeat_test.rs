//! Hosted share cadence, retry-gate, and failure-classification scenarios.

use super::{
    membership::ShareMembershipFailureTurn,
    registry_heartbeat_due::ShareHeartbeatDueTurn,
    registry_heartbeat_settlement::{driver_failure, rediscovery_failure},
    registry_test::{add_membership, registry_with_membership, settle_assignment},
};
use crate::{
    driver::{
        ConsumerGroupHeartbeatDriverFailureKind, share_group_heartbeat::ShareGroupHeartbeatRoute,
    },
    protocol::consumer::share_group::share_group_heartbeat_success_for_test,
};
use kafka_client_core::{
    Moment, ShareGroupHeartbeatFailure, ShareGroupHeartbeatPhase, ShareGroupHeartbeatRequestKind,
};
use std::sync::Arc;

#[test]
fn accepted_assignment_arms_and_prepares_one_steady_heartbeat() {
    let (mut registry, group_id, clock, capture) = registry_with_membership();
    let entry = registry
        .entry_mut(group_id)
        .unwrap_or_else(|| panic!("entry"));
    let member = Arc::clone(entry.member());
    entry
        .membership
        .as_mut()
        .unwrap_or_else(|| panic!("membership"))
        .settle_success(
            capture.now(),
            share_group_heartbeat_success_for_test(
                Some(&member),
                1,
                5_000,
                vec![([7; 16], vec![0])],
            ),
        )
        .unwrap_or_else(|error| panic!("success: {error:?}"));
    let schedule = entry
        .membership
        .as_ref()
        .and_then(|membership| membership.machine().schedule())
        .unwrap_or_else(|| panic!("schedule"));

    assert_eq!(
        registry
            .prepare_one_heartbeat_due(Moment::from_tick(schedule.deadline().tick()), &clock)
            .unwrap_or_else(|error| panic!("due: {error:?}")),
        ShareHeartbeatDueTurn::Progress
    );
    let prepared = registry
        .entry(group_id)
        .and_then(|entry| entry.membership.as_ref())
        .and_then(super::ShareMembershipInterpreter::prepared)
        .unwrap_or_else(|| panic!("prepared"));
    assert_eq!(prepared.kind, ShareGroupHeartbeatRequestKind::Steady);
    assert_eq!(
        prepared
            .member_epoch
            .map(kafka_client_core::ShareGroupMemberEpoch::get),
        Some(1)
    );
}

#[test]
fn rediscovery_needs_permission_and_positive_delay_in_either_order() {
    let (mut registry, group_id, clock, capture) = registry_with_membership();
    let entry = registry
        .entry_mut(group_id)
        .unwrap_or_else(|| panic!("entry"));
    let turn = entry
        .membership
        .as_mut()
        .unwrap_or_else(|| panic!("membership"))
        .settle_failure(
            capture.now(),
            &clock,
            ShareGroupHeartbeatFailure::Broker(16),
        )
        .unwrap_or_else(|error| panic!("rediscovery: {error:?}"));
    let ShareMembershipFailureTurn::Rediscovery(schedule) = turn else {
        panic!("rediscovery schedule")
    };
    registry
        .apply_invalidation_terminal(
            group_id,
            Ok(crate::driver::share_group_heartbeat::ShareCoordinatorInvalidationPermission::Applied),
        )
        .unwrap_or_else(|error| panic!("permission: {error:?}"));
    assert_eq!(
        registry
            .prepare_one_heartbeat_due(Moment::from_tick(schedule.not_before().tick()), &clock)
            .unwrap_or_else(|error| panic!("retry due: {error:?}")),
        ShareHeartbeatDueTurn::Progress
    );
    assert!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.membership.as_ref())
            .is_some_and(super::ShareMembershipInterpreter::is_ready_to_submit)
    );
}

#[test]
fn route_less_transport_retries_after_delay_without_an_invalidation_owner() {
    let (mut registry, group_id, clock, capture) = registry_with_membership();
    registry
        .begin_rediscovery(
            0,
            capture.now(),
            &clock,
            ShareGroupHeartbeatFailure::CoordinatorUnavailable,
            ShareGroupHeartbeatRoute::without_token_for_test(),
        )
        .unwrap_or_else(|error| panic!("route-less rediscovery: {error:?}"));
    assert_eq!(registry.invalidations.retained_count(), 0);
    let schedule = registry
        .entry(group_id)
        .and_then(|entry| entry.membership.as_ref())
        .and_then(|membership| membership.machine().retry_schedule())
        .unwrap_or_else(|| panic!("retry schedule"));
    assert_eq!(
        registry
            .prepare_one_heartbeat_due(Moment::from_tick(schedule.not_before().tick()), &clock,)
            .unwrap_or_else(|error| panic!("retry due: {error:?}")),
        ShareHeartbeatDueTurn::Progress
    );
    assert!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.membership.as_ref())
            .is_some_and(super::ShareMembershipInterpreter::is_ready_to_submit)
    );
}

#[test]
fn due_selection_skips_an_earlier_member_with_only_future_work() {
    let (mut registry, first_id, clock, first_capture) = registry_with_membership();
    let (second_id, second_capture) = add_membership(&mut registry, &clock, "workers-2");
    settle_assignment(&mut registry, first_id, first_capture.now(), 10_000);
    settle_assignment(&mut registry, second_id, second_capture.now(), 1_000);
    let second_due = registry
        .entry(second_id)
        .and_then(|entry| entry.membership.as_ref())
        .and_then(|membership| membership.machine().schedule())
        .unwrap_or_else(|| panic!("second schedule"));

    assert_eq!(
        registry
            .prepare_one_heartbeat_due(Moment::from_tick(second_due.deadline().tick()), &clock,)
            .unwrap_or_else(|error| panic!("due: {error:?}")),
        ShareHeartbeatDueTurn::Progress
    );
    assert!(
        registry
            .entry(first_id)
            .and_then(|entry| entry.membership.as_ref())
            .and_then(super::ShareMembershipInterpreter::prepared)
            .is_none()
    );
    assert_eq!(
        registry
            .entry(second_id)
            .and_then(|entry| entry.membership.as_ref())
            .and_then(super::ShareMembershipInterpreter::prepared)
            .map(|prepared| prepared.kind),
        Some(ShareGroupHeartbeatRequestKind::Steady)
    );
}

#[test]
fn late_invalidation_terminal_releases_ownership_after_retry_deadline() {
    let (mut registry, group_id, clock, capture) = registry_with_membership();
    registry
        .entry_mut(group_id)
        .and_then(|entry| entry.membership.as_mut())
        .unwrap_or_else(|| panic!("membership"))
        .settle_failure(
            capture.now(),
            &clock,
            ShareGroupHeartbeatFailure::Broker(16),
        )
        .unwrap_or_else(|error| panic!("rediscovery: {error:?}"));

    assert_eq!(
        registry
            .prepare_one_heartbeat_due(Moment::from_tick(capture.deadline().tick()), &clock,)
            .unwrap_or_else(|error| panic!("deadline: {error:?}")),
        ShareHeartbeatDueTurn::Progress
    );
    assert_eq!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.membership.as_ref())
            .map(|membership| membership.machine().phase()),
        Some(ShareGroupHeartbeatPhase::Fatal)
    );
    registry
        .apply_invalidation_terminal(
            group_id,
            Ok(crate::driver::share_group_heartbeat::ShareCoordinatorInvalidationPermission::Applied),
        )
        .unwrap_or_else(|error| panic!("late terminal: {error:?}"));
}

#[test]
fn retryable_response_observed_at_deadline_terminalizes_atomically() {
    let (mut registry, group_id, clock, capture) = registry_with_membership();
    let membership = registry
        .entry_mut(group_id)
        .and_then(|entry| entry.membership.as_mut())
        .unwrap_or_else(|| panic!("membership"));
    assert_eq!(
        membership
            .settle_failure(
                Moment::from_tick(capture.deadline().tick()),
                &clock,
                ShareGroupHeartbeatFailure::Broker(14),
            )
            .unwrap_or_else(|error| panic!("terminal: {error:?}")),
        ShareMembershipFailureTurn::Terminal
    );
    assert_eq!(
        membership.machine().phase(),
        ShareGroupHeartbeatPhase::Fatal
    );
    assert_eq!(
        membership.startup_failure(),
        Some(ShareGroupHeartbeatFailure::DeadlineElapsed)
    );
    assert!(membership.prepared().is_none());
}

#[test]
fn driver_failures_map_without_granting_unknown_retry_authority() {
    assert_eq!(
        driver_failure(ConsumerGroupHeartbeatDriverFailureKind::Transport),
        ShareGroupHeartbeatFailure::CoordinatorUnavailable
    );
    assert_eq!(
        driver_failure(ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed),
        ShareGroupHeartbeatFailure::DeadlineElapsed
    );
    assert_eq!(
        driver_failure(ConsumerGroupHeartbeatDriverFailureKind::ResponseTooLarge),
        ShareGroupHeartbeatFailure::InvalidResponse
    );
    assert_eq!(
        driver_failure(ConsumerGroupHeartbeatDriverFailureKind::DriverRejected),
        ShareGroupHeartbeatFailure::Execution
    );
}

#[test]
fn only_route_evidenced_steady_deadlines_authorize_rediscovery() {
    assert_eq!(
        rediscovery_failure(
            ShareGroupHeartbeatRequestKind::Steady,
            ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed,
            true,
        ),
        Some(ShareGroupHeartbeatFailure::CoordinatorUnavailable)
    );
    assert_eq!(
        rediscovery_failure(
            ShareGroupHeartbeatRequestKind::Steady,
            ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed,
            false,
        ),
        None
    );
    assert_eq!(
        rediscovery_failure(
            ShareGroupHeartbeatRequestKind::Join,
            ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed,
            true,
        ),
        None
    );
}
