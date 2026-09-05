//! Route-less steady expiry revokes exact membership and starts one bounded join.

use kafka_client_core::{Moment, ShareGroupHeartbeatPhase, ShareGroupHeartbeatRequestKind};

use super::{
    registry_heartbeat_settlement::rediscovery_failure,
    registry_test::{registry_with_membership, settle_assignment},
};
use crate::driver::{
    ConsumerGroupHeartbeatDriverFailureKind, share_group_heartbeat::ShareGroupHeartbeatRoute,
};

#[test]
fn expired_steady_lookup_rejoins_without_fabricating_route_invalidation() {
    let (mut registry, group_id, clock, capture) = registry_with_membership();
    settle_assignment(&mut registry, group_id, capture.now(), 5_000);
    let owner = registry
        .entry(group_id)
        .and_then(|entry| entry.membership.as_ref())
        .unwrap_or_else(|| panic!("membership"));
    let member = owner.machine().member_id();
    let due = owner
        .machine()
        .schedule()
        .unwrap_or_else(|| panic!("schedule"));
    registry
        .prepare_one_heartbeat_due(Moment::from_tick(due.deadline().tick()), &clock)
        .unwrap_or_else(|error| panic!("steady due: {error:?}"));
    let rejected = registry
        .entry(group_id)
        .and_then(|entry| entry.membership.as_ref())
        .and_then(super::ShareMembershipInterpreter::prepared)
        .unwrap_or_else(|| panic!("steady attempt"));
    let failure = rediscovery_failure(
        rejected.kind,
        ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed,
    )
    .unwrap_or_else(|| panic!("background expiry must recover"));
    registry
        .begin_rediscovery(
            0,
            Moment::from_tick(rejected.deadline.core().tick()),
            &clock,
            failure,
            ShareGroupHeartbeatRoute::without_token_for_test(),
        )
        .unwrap_or_else(|error| panic!("route-less recovery: {error:?}"));
    assert_eq!(registry.invalidations.retained_count(), 0);
    let owner = registry
        .entry(group_id)
        .and_then(|entry| entry.membership.as_ref())
        .unwrap_or_else(|| panic!("membership"));
    let replacement = owner.prepared().unwrap_or_else(|| panic!("replacement"));
    let retry = owner
        .machine()
        .retry_schedule()
        .unwrap_or_else(|| panic!("retry"));
    assert_ne!(replacement.attempt, rejected.attempt);
    assert_eq!(replacement.kind, ShareGroupHeartbeatRequestKind::Join);
    assert_eq!(replacement.member_epoch, None);
    assert_eq!(replacement.assignment_generation, None);
    assert!(replacement.deadline.core() > rejected.deadline.core());
    assert_eq!(owner.machine().member_id(), member);
    assert_eq!(owner.machine().phase(), ShareGroupHeartbeatPhase::Joining);
    assert!(owner.machine().fatal().is_none());
    assert!(owner.activated_assignment().is_none());
    assert!(!owner.is_ready_to_submit());
    assert!(retry.not_before().tick() > rejected.deadline.core().tick());
    registry
        .prepare_one_heartbeat_due(Moment::from_tick(retry.not_before().tick()), &clock)
        .unwrap_or_else(|error| panic!("retry due: {error:?}"));
    assert!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.membership.as_ref())
            .is_some_and(super::ShareMembershipInterpreter::is_ready_to_submit)
    );
    assert_eq!(
        registry.entry(group_id).and_then(|entry| entry.start),
        Some(capture)
    );
}

#[test]
fn unavailable_invalidation_preserves_retry_delay_and_original_deadline() {
    use crate::driver::share_group_heartbeat::ShareCoordinatorInvalidationPermission;
    use kafka_client_core::ShareGroupHeartbeatFailure;

    for expire in [false, true] {
        let (mut registry, group_id, clock, capture) = registry_with_membership();
        let owner = registry
            .entry_mut(group_id)
            .and_then(|entry| entry.membership.as_mut())
            .unwrap_or_else(|| panic!("membership"));
        owner
            .settle_failure(
                capture.now(),
                &clock,
                ShareGroupHeartbeatFailure::Broker(16),
            )
            .unwrap_or_else(|error| panic!("schedule rediscovery: {error:?}"));
        let prepared = owner.prepared().unwrap_or_else(|| panic!("prepared"));
        let retry = owner
            .machine()
            .retry_schedule()
            .unwrap_or_else(|| panic!("retry"));
        registry
            .apply_invalidation_terminal(
                group_id,
                Ok(ShareCoordinatorInvalidationPermission::Unavailable),
            )
            .unwrap_or_else(|error| panic!("withdrawn route: {error:?}"));
        let owner = registry
            .entry(group_id)
            .and_then(|entry| entry.membership.as_ref())
            .unwrap_or_else(|| panic!("membership"));
        assert_eq!(owner.prepared(), Some(prepared));
        assert_eq!(prepared.deadline.core(), capture.deadline());
        assert!(!owner.is_ready_to_submit());
        let due = if expire {
            retry.deadline()
        } else {
            retry.not_before()
        };
        registry
            .prepare_one_heartbeat_due(Moment::from_tick(due.tick()), &clock)
            .unwrap_or_else(|error| panic!("retry due: {error:?}"));
        let owner = registry
            .entry(group_id)
            .and_then(|entry| entry.membership.as_ref())
            .unwrap_or_else(|| panic!("membership"));
        if expire {
            assert!(owner.prepared().is_none());
            assert_eq!(
                owner.startup_failure(),
                Some(ShareGroupHeartbeatFailure::DeadlineElapsed)
            );
        } else {
            assert_eq!(owner.prepared(), Some(prepared));
            assert!(owner.is_ready_to_submit());
        }
    }
}
