//! Hosted broker-session opening, tracked submission, close, and recovery evidence.

use std::time::Duration;

use super::{
    registry_close::ShareConsumerCloseTurn,
    registry_fetch_routing_test::{
        registry_with_routable_membership, route_assignment, routed_driver, settle_partition_three,
    },
    registry_fetch_sessions::ShareFetchSessionsHostTurn,
};

#[test]
fn routed_assignment_opens_and_submits_one_tracked_session() {
    let (mut registry, group_id, clock, capture) = registry_with_routable_membership();
    settle_partition_three(&mut registry, group_id, capture.now());
    let (mut broker, mut driver) = routed_driver();
    route_assignment(
        &mut registry,
        group_id,
        &clock,
        capture,
        &mut broker,
        &mut driver,
    );
    let now = clock.now().unwrap_or_else(|error| panic!("now: {error:?}"));

    assert_eq!(
        registry
            .turn_one_fetch_sessions(now, &clock, &driver)
            .unwrap_or_else(|error| panic!("open sessions: {error:?}")),
        ShareFetchSessionsHostTurn::Progress
    );
    let entry = registry.entry(group_id).unwrap_or_else(|| panic!("entry"));
    assert!(entry.fetch().routed().is_none());
    assert_eq!(
        entry
            .fetch()
            .sessions()
            .map(super::fetch_session_set::ShareFetchSessionSet::len),
        Some(1)
    );
    assert_eq!(
        registry
            .turn_one_fetch_sessions(now, &clock, &driver)
            .unwrap_or_else(|error| panic!("submit session: {error:?}")),
        ShareFetchSessionsHostTurn::Progress
    );

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover: {error:?}"));
}

#[test]
fn close_releases_prepared_sessions_before_membership_leave() {
    let (mut registry, group_id, clock, capture) = registry_with_routable_membership();
    settle_partition_three(&mut registry, group_id, capture.now());
    let (mut broker, mut driver) = routed_driver();
    route_assignment(
        &mut registry,
        group_id,
        &clock,
        capture,
        &mut broker,
        &mut driver,
    );
    let now = clock.now().unwrap_or_else(|error| panic!("now: {error:?}"));
    assert_eq!(
        registry
            .turn_one_fetch_sessions(now, &clock, &driver)
            .unwrap_or_else(|error| panic!("open sessions: {error:?}")),
        ShareFetchSessionsHostTurn::Progress
    );

    registry.request_control_close(capture);
    assert_eq!(
        registry
            .turn_one_close(now)
            .unwrap_or_else(|error| panic!("blocked close: {error:?}")),
        ShareConsumerCloseTurn::Blocked
    );
    assert_eq!(
        registry
            .turn_one_fetch_sessions(now, &clock, &driver)
            .unwrap_or_else(|error| panic!("release sessions: {error:?}")),
        ShareFetchSessionsHostTurn::Progress
    );
    assert!(
        registry
            .entry(group_id)
            .is_some_and(|entry| entry.fetch().sessions().is_none())
    );
    assert_eq!(
        registry
            .turn_one_close(now)
            .unwrap_or_else(|error| panic!("begin leave: {error:?}")),
        ShareConsumerCloseTurn::Blocked
    );

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover: {error:?}"));
}
