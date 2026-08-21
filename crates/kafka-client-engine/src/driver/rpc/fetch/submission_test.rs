//! Partition-routed long-poll `Fetch` submission scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, RouteKind, RoutedCall, TrafficClass};
use kafka_wire::{FetchRequest, FetchResponse};

use crate::{EngineConfig, driver::DriverOwner, protocol::fetch::FETCH_NAME_ROUTE_MAX_VERSION};

use super::{
    route::BrokerId,
    routed_response_broker_test::RoutedBroker,
    submission::{FetchSubmitError, fetch_options, fetch_options_for_request},
};

#[test]
fn options_preserve_original_deadline_long_poll_lane_and_name_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = fetch_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::LongPoll);
    assert_eq!(
        options.maximum_version(),
        Some(ApiVersion::new(FETCH_NAME_ROUTE_MAX_VERSION))
    );
}

#[test]
fn established_session_requests_require_fetch_v7_while_initial_can_fallback() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let mut initial = FetchRequest::default();
    initial.session_epoch = 0;
    assert_eq!(
        fetch_options_for_request(deadline, &initial).minimum_version(),
        None
    );

    let mut incremental = FetchRequest::default();
    incremental.session_id = 91;
    incremental.session_epoch = 3;
    assert_eq!(
        fetch_options_for_request(deadline, &incremental).minimum_version(),
        Some(ApiVersion::new(7))
    );
}

#[test]
fn accepted_submission_returns_a_tracked_partition_call() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let call = owner
        .submit_tracked_fetch("events", 3, FetchRequest::default(), deadline)
        .unwrap_or_else(|error| panic!("tracked Fetch admission: {error}"));

    assert!(call.try_result().is_none());
    assert_tracked_fetch(&call);
    drop(call);
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn accepted_exact_broker_submission_returns_a_tracked_call() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let broker_id = BrokerId::new(3).unwrap_or_else(|error| panic!("broker ID: {error}"));
    let call = owner
        .submit_tracked_broker_fetch(broker_id, FetchRequest::default(), deadline)
        .unwrap_or_else(|error| panic!("tracked exact-broker Fetch admission: {error}"));

    assert!(call.try_result().is_none());
    assert_tracked_fetch(&call);
    drop(call);
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn exact_broker_submission_reaches_the_selected_loopback_broker() {
    let mut broker = RoutedBroker::new();
    let mut owner = DriverOwner::build(&EngineConfig::new(vec![broker.endpoint()]))
        .unwrap_or_else(|error| panic!("build exact-broker driver: {error}"));
    RoutedBroker::await_seed(&mut owner);
    broker.install_cluster(&mut owner);
    let call = owner
        .submit_tracked_broker_fetch(
            BrokerId::new(1).unwrap_or_else(|error| panic!("broker ID: {error}")),
            FetchRequest::default(),
            Instant::now() + Duration::from_secs(60),
        )
        .unwrap_or_else(|error| panic!("tracked exact-broker Fetch admission: {error}"));

    assert_eq!(broker.complete_fetch(&mut owner).value(), 12);
    let outcome = call
        .wait()
        .unwrap_or_else(|error| panic!("exact-broker completion: {error}"));
    assert!(outcome.result().is_ok());
    assert_eq!(
        outcome.route_failure_token().map(|token| token.kind()),
        Some(RouteKind::Broker)
    );
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn invalid_route_facts_are_rejected_before_driver_ownership() {
    let owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let empty_topic = owner
        .submit_tracked_fetch("", 0, FetchRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("empty topic must be rejected"));
    assert!(matches!(empty_topic, FetchSubmitError::InvalidTopic(_)));

    let negative_partition = owner
        .submit_tracked_fetch("events", -1, FetchRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("negative partition must be rejected"));
    assert!(matches!(
        negative_partition,
        FetchSubmitError::InvalidPartition(_)
    ));
}

fn assert_tracked_fetch(_call: &RoutedCall<FetchResponse>) {}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
