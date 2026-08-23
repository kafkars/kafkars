//! Public share-state projection and retained startup-terminal scenarios.

use kafka_client_core::{Moment, ShareGroupHeartbeatFailure};

use super::registry_heartbeat_test::{registry_with_membership, settle_assignment};

#[test]
fn state_copies_exact_epoch_generation_topic_and_partition() {
    let (mut registry, group_id, _clock, capture) = registry_with_membership();
    assert_eq!(
        registry
            .share_state(group_id)
            .unwrap_or_else(|error| panic!("joining state: {error:?}")),
        None
    );

    settle_assignment(&mut registry, group_id, capture.now(), 5_000);
    let state = registry
        .share_state(group_id)
        .unwrap_or_else(|error| panic!("stable state: {error:?}"))
        .unwrap_or_else(|| panic!("stable assignment"));
    assert_eq!(state.member_epoch(), 1);
    assert_eq!(state.assignment_generation(), 1);
    assert_eq!(state.partitions().len(), 1);
    assert_eq!(state.partitions()[0].topic(), "jobs");
    assert_eq!(state.partitions()[0].partition(), 0);
}

#[test]
fn startup_failure_preserves_the_exact_pre_success_terminal() {
    let (mut registry, group_id, clock, capture) = registry_with_membership();
    registry
        .entry_mut(group_id)
        .and_then(|entry| entry.membership.as_mut())
        .unwrap_or_else(|| panic!("membership"))
        .settle_failure(
            Moment::from_tick(capture.deadline().tick()),
            &clock,
            ShareGroupHeartbeatFailure::Broker(15),
        )
        .unwrap_or_else(|error| panic!("terminal: {error:?}"));
    assert_eq!(
        registry
            .startup_failure(group_id)
            .unwrap_or_else(|error| panic!("startup failure: {error:?}")),
        Some(ShareGroupHeartbeatFailure::DeadlineElapsed)
    );
}
