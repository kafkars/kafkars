//! Hosted broker-session opening, tracked submission, close, and recovery evidence.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use super::{
    close_state::ShareConsumerCloseTerminal,
    fetch_session_set::owner_test::staged_session_set_for_test,
    registry_close::ShareConsumerCloseTurn,
    registry_fetch_routing_test::{
        add_routable_membership, registry_with_routable_membership, route_assignment,
        routed_driver, settle_partition_three,
    },
    registry_fetch_sessions::ShareFetchSessionsHostTurn,
};

#[test]
fn retained_share_delivery_does_not_hide_the_original_close_deadline() {
    let (mut registry, group_id, clock, _capture) = registry_with_routable_membership();
    let displaced = registry
        .entry_mut(group_id)
        .unwrap_or_else(|| panic!("entry"))
        .fetch_mut()
        .install_sessions(staged_session_set_for_test(41));
    assert!(displaced.is_none());
    let delivery = registry
        .take_delivery(group_id, kafka_client_core::Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("delivery: {error:?}"))
        .unwrap_or_else(|| panic!("staged delivery"));
    let capture = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("close deadline: {error:?}"));
    let mut observer = registry
        .begin_explicit_close(group_id, capture)
        .unwrap_or_else(|error| panic!("close: {error:?}"));
    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(Pin::new(&mut observer).poll(&mut context), Poll::Pending);
    let elapsed = kafka_client_core::Moment::from_tick(capture.deadline().tick());
    for _turn in 0..2 {
        registry
            .turn_one_close(elapsed)
            .unwrap_or_else(|error| panic!("close turn: {error:?}"));
    }
    let publication_bound = std::time::Instant::now() + Duration::from_secs(1);
    let observed = loop {
        let observed = Pin::new(&mut observer).poll(&mut context);
        if observed.is_ready() || std::time::Instant::now() >= publication_bound {
            break observed;
        }
        std::thread::yield_now();
    };
    assert!(
        registry
            .entry(group_id)
            .is_some_and(|entry| entry.fetch().sessions().is_some())
    );
    assert!(registry.retained_name_bytes() > 0);
    let close = registry
        .entry(group_id)
        .and_then(super::entry::ShareConsumerEntry::close)
        .unwrap_or_else(|| panic!("close owner"));
    assert_eq!(close.completion_id(), None);
    assert_eq!(close.next_deadline(), None);
    assert_eq!(
        registry.turn_one_close(elapsed),
        Ok(ShareConsumerCloseTurn::Blocked)
    );
    registry
        .reclaim_delivery(delivery)
        .unwrap_or_else(|_failure| panic!("exact reclaim"));
    let mut driver = crate::driver::DriverOwner::build(&crate::EngineConfig::new(vec![
        "127.0.0.1:1".to_owned(),
    ]))
    .unwrap_or_else(|error| panic!("driver: {error}"));
    for _turn in 0..8 {
        registry
            .turn_one_fetch_sessions(elapsed, &clock, &driver)
            .unwrap_or_else(|error| panic!("session drain: {error:?}"));
        registry
            .turn_one_close(elapsed)
            .unwrap_or_else(|error| panic!("close drain: {error:?}"));
        if registry.entry(group_id).is_none() {
            break;
        }
    }
    assert!(
        registry.entry(group_id).is_none(),
        "exact reclaim permits physical removal"
    );
    driver
        .shutdown_with_turn_limit(32, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover: {error:?}"));
    assert_eq!(registry.retained_name_bytes(), 0);
    assert_eq!(
        observed,
        Poll::Ready(Ok(ShareConsumerCloseTerminal::Failed(
            kafka_client_core::ShareGroupHeartbeatFailure::DeadlineElapsed
        )))
    );
}

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
