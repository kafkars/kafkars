//! Hosted broker-session opening, tracked submission, close, and recovery evidence.

use std::time::Duration;

use super::{
    registry_close::ShareConsumerCloseTurn,
    registry_fetch_routing_test::{
        add_routable_membership, registry_with_routable_membership, route_assignment,
        routed_driver, settle_partition_three,
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
fn pending_first_member_does_not_starve_a_second_member_session() {
    let (mut registry, first_id, clock, first_capture) = registry_with_routable_membership();
    let (second_id, second_capture) = add_routable_membership(&mut registry, &clock, "workers");
    settle_partition_three(&mut registry, first_id, first_capture.now());
    settle_partition_three(&mut registry, second_id, second_capture.now());
    let (mut broker, mut driver) = routed_driver();
    route_assignment(
        &mut registry,
        first_id,
        &clock,
        first_capture,
        &mut broker,
        &mut driver,
    );
    for _turn in 0..32 {
        let _turn = registry
            .turn_one_fetch_routing(second_capture.now(), &clock, &driver)
            .unwrap_or_else(|error| panic!("route second assignment: {error:?}"));
        if registry
            .entry(second_id)
            .is_some_and(|entry| entry.fetch().routed().is_some())
        {
            break;
        }
        driver
            .turn(Duration::from_millis(100))
            .unwrap_or_else(|error| panic!("drive cached route: {error}"));
    }
    assert!(
        registry
            .entry(second_id)
            .is_some_and(|entry| entry.fetch().routed().is_some())
    );
    let now = clock.now().unwrap_or_else(|error| panic!("now: {error:?}"));
    for phase in ["open", "prepare", "submit"] {
        assert_eq!(
            registry
                .turn_one_fetch_sessions(now, &clock, &driver)
                .unwrap_or_else(|error| panic!("{phase} first session: {error:?}")),
            ShareFetchSessionsHostTurn::Progress
        );
    }

    assert_eq!(
        registry
            .turn_one_fetch_sessions(now, &clock, &driver)
            .unwrap_or_else(|error| panic!("open second session: {error:?}")),
        ShareFetchSessionsHostTurn::Progress
    );
    assert!(
        registry
            .entry(second_id)
            .is_some_and(|entry| entry.fetch().sessions().is_some())
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
