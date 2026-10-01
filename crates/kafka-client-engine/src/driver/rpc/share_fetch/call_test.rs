//! Share calls and causal route recovery retain exact terminal ownership.

use std::time::{Duration, Instant};

use kafka_client_core::{Deadline, Moment, ShareFetchBrokerId};
use kafka_driver::{BrokerId, Route, RouteFailureToken, SubmitError, TrafficClass};
use kafka_wire::FetchRequest;

use crate::driver::rpc::fetch::routed_response_broker_test::RoutedBroker;
use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::ShareFetchCall;
use super::terminal_test::prepared;

#[test]
fn completion_failure_returns_exact_response_correlation() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let broker = ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("valid broker"));
    let mut call = ShareFetchCall::submit(
        &driver,
        broker,
        prepared(),
        Moment::from_tick(10),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(30),
            Instant::now() + Duration::from_secs(1),
        ),
    )
    .unwrap_or_else(|_failure| panic!("accepted ShareFetch call"));
    drop(driver);

    let failure = call
        .try_terminal()
        .unwrap_or_else(|| panic!("completion must be terminal"))
        .err()
        .unwrap_or_else(|| panic!("driver shutdown must fail completion"));
    let (evidence, kind) = failure.into_parts();
    assert_eq!(kind, super::ShareFetchCompletionErrorKind::Closed);
    let super::ShareFetchCallEvidence { correlation, .. } = evidence;
    assert!(correlation.contains(topic_id(), 0));
}

#[test]
fn broker_recovery_waits_for_causal_metadata_across_a_seed_gap() {
    let (mut peer, mut driver, token) = routed_token();
    peer.cut_seed();
    driver
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("observe seed EOF: {error}"));
    let route = super::ShareFetchRoute::new(
        ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("broker")),
        Some(token),
    );
    let mut recovery = super::ShareFetchRouteRefresh::try_new(
        route,
        Deadline::from_tick(60_000_000_000),
        Instant::now() + Duration::from_secs(60),
        "events",
    )
    .unwrap_or_else(|_route| panic!("routed recovery"));
    assert_eq!(
        recovery.poll(&driver, Moment::from_tick(1)),
        super::ShareFetchRouteRefreshPoll::Progress
    );
    driver
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("admit seed-gap recovery: {error}"));
    assert_eq!(
        recovery.poll(&driver, Moment::from_tick(2)),
        super::ShareFetchRouteRefreshPoll::Pending
    );
    peer.reconnect_seed(&mut driver);
    assert_eq!(
        recovery.poll(&driver, Moment::from_tick(2)),
        super::ShareFetchRouteRefreshPoll::Pending,
        "neither cached metadata nor reconnect alone completes causal recovery"
    );
    peer.install_topic(&mut driver, 1);
    let ready = (0..32).any(|_| {
        driver
            .turn(Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("settle causal recovery: {error}"));
        recovery.poll(&driver, Moment::from_tick(3)) == super::ShareFetchRouteRefreshPoll::Ready
    });
    assert!(ready, "fresh metadata must settle recovery");
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown recovered driver: {error}"));
}

#[test]
fn causal_recovery_retains_its_token_through_full_admission() {
    let (mut peer, mut driver, token) = routed_token();
    let mut recovery = recovery(
        token,
        60_000_000_000,
        Instant::now() + Duration::from_secs(60),
    );
    assert!(fill_control_mailbox(&driver) > 0);
    for tick in 1..=4 {
        assert_eq!(
            recovery.poll(&driver, Moment::from_tick(tick)),
            super::ShareFetchRouteRefreshPoll::Pending
        );
    }
    driver
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("drain mailbox: {error}"));
    assert_eq!(
        recovery.poll(&driver, Moment::from_tick(5)),
        super::ShareFetchRouteRefreshPoll::Progress
    );
    driver
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("admit causal lookup: {error}"));
    assert_eq!(
        recovery.poll(&driver, Moment::from_tick(6)),
        super::ShareFetchRouteRefreshPoll::Pending
    );
    peer.install_topic(&mut driver, 1);
    assert_eq!(
        recovery.poll(&driver, Moment::from_tick(7)),
        super::ShareFetchRouteRefreshPoll::Ready
    );
    assert_eq!(
        recovery.poll(&driver, Moment::from_tick(8)),
        super::ShareFetchRouteRefreshPoll::Ready
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
}

#[test]
fn full_admission_cannot_restart_the_recovery_deadline() {
    let (_peer, mut driver, token) = routed_token();
    let mut recovery = recovery(token, 3, Instant::now() + Duration::from_secs(60));
    fill_control_mailbox(&driver);
    for tick in 1..=2 {
        assert_eq!(
            recovery.poll(&driver, Moment::from_tick(tick)),
            super::ShareFetchRouteRefreshPoll::Pending
        );
    }
    for tick in 3..=4 {
        assert_eq!(
            recovery.poll(&driver, Moment::from_tick(tick)),
            super::ShareFetchRouteRefreshPoll::Failed
        );
    }
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
}

#[test]
fn causal_metadata_must_exceed_the_assignment_generation_floor() {
    for (floor, expected) in [
        (0, super::ShareFetchRouteRefreshPoll::Ready),
        (u64::MAX, super::ShareFetchRouteRefreshPoll::Failed),
    ] {
        let (mut peer, mut driver, token) = routed_token();
        let route = super::ShareFetchRoute::new(
            ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("broker")),
            Some(token),
        );
        let mut recovery = super::ShareFetchRouteRefresh::try_new_with_metadata(
            route,
            Deadline::from_tick(60_000_000_000),
            Instant::now() + Duration::from_secs(60),
            "events",
            kafka_client_core::partitioning::TopicMetadataGeneration::from_raw(floor),
        )
        .unwrap_or_else(|_route| panic!("routed recovery"));
        assert_eq!(
            recovery.poll(&driver, Moment::from_tick(1)),
            super::ShareFetchRouteRefreshPoll::Progress
        );
        peer.install_topic(&mut driver, 1);
        assert_eq!(recovery.poll(&driver, Moment::from_tick(2)), expected);
        driver
            .shutdown_with_turn_limit(64, Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("shutdown: {error}"));
    }
}

#[test]
fn admitted_causal_lookup_expires_without_restarting_its_transport_deadline() {
    let (peer, mut driver, token) = routed_token();
    let transport_deadline = Instant::now() + Duration::from_millis(20);
    let mut recovery = recovery(token, 60_000_000_000, transport_deadline);
    assert_eq!(
        recovery.poll(&driver, Moment::from_tick(1)),
        super::ShareFetchRouteRefreshPoll::Progress
    );
    let expired = (0..32).any(|_| {
        driver
            .turn(Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("drive deadline: {error}"));
        recovery.poll(&driver, Moment::from_tick(2)) == super::ShareFetchRouteRefreshPoll::Failed
    });
    assert!(
        expired,
        "accepted metadata waiter remains bounded by its original transport deadline"
    );
    drop(peer);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
}

fn recovery(
    token: RouteFailureToken,
    deadline: u64,
    transport_deadline: Instant,
) -> super::ShareFetchRouteRefresh {
    let route = super::ShareFetchRoute::new(
        ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("broker")),
        Some(token),
    );
    super::ShareFetchRouteRefresh::try_new(
        route,
        Deadline::from_tick(deadline),
        transport_deadline,
        "events",
    )
    .unwrap_or_else(|_route| panic!("causal recovery"))
}

fn fill_control_mailbox(driver: &DriverOwner) -> usize {
    for accepted in 0..4096 {
        match driver.driver.snapshot() {
            Ok(call) => drop(call),
            Err(SubmitError::Full) => return accepted,
            Err(error) => panic!("fill control mailbox: {error}"),
        }
    }
    panic!("bounded driver control mailbox must refuse admission");
}

fn routed_token() -> (RoutedBroker, DriverOwner, RouteFailureToken) {
    let mut peer = RoutedBroker::new();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![peer.endpoint()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    RoutedBroker::await_seed(&mut driver);
    peer.install_cluster(&mut driver);
    let mut cached = crate::driver::TopicPartitionCountCall::submit(
        &driver,
        "events",
        Instant::now() + Duration::from_secs(60),
    )
    .unwrap_or_else(|error| panic!("cached view: {error}"));
    peer.install_topic(&mut driver, 1);
    assert!(matches!(cached.try_terminal(), Some(Ok(_))));
    let call = driver
        .driver
        .request_tracked_in(
            TrafficClass::LongPoll,
            Route::Broker {
                broker_id: BrokerId::new(1).unwrap_or_else(|error| panic!("broker: {error}")),
            },
            FetchRequest::default(),
            Duration::from_secs(60),
        )
        .unwrap_or_else(|error| panic!("tracked broker request: {error}"));
    peer.complete_fetch(&mut driver);
    let outcome = (0..32)
        .find_map(|_| {
            driver
                .turn(Duration::from_millis(10))
                .unwrap_or_else(|error| panic!("settle tracked request: {error}"));
            call.try_result()
        })
        .unwrap_or_else(|| panic!("tracked broker request must settle"))
        .unwrap_or_else(|error| panic!("tracked broker completion: {error}"));
    let (_, _, token) = outcome.into_parts();
    (
        peer,
        driver,
        token.unwrap_or_else(|| panic!("broker route token")),
    )
}

fn topic_id() -> [u8; 16] {
    let mut id = [0; 16];
    id[0] = 1;
    id
}
