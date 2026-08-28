//! Tracked name- and exact-broker-routed Produce submission scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{DeliveryStatus, ProducerAttemptFailureKind};
use kafka_driver::{ApiKey, ApiVersion, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{ProduceRequest, ProduceResponse};

use crate::EngineConfig;

use super::{
    super::DriverOwner,
    submission::{ProduceSubmitError, produce_options},
};

#[test]
fn produce_options_preserve_deadline_lane_and_version_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = produce_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Bulk);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(3)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(12)));
}

#[test]
fn accepted_submission_returns_the_token_retaining_tracked_call() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let call = owner
        .submit_tracked_produce("orders", 3, ProduceRequest::default(), deadline)
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"));

    assert!(call.try_result().is_none());
    assert_tracked_produce(&call);
    drop(call);
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn accepted_exact_broker_submission_returns_a_tracked_call() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let call = owner
        .submit_tracked_produce_to_broker(7, ProduceRequest::default(), deadline)
        .unwrap_or_else(|error| panic!("exact-broker Produce admission: {error}"));

    assert!(call.try_result().is_none());
    assert_tracked_produce(&call);
    drop(call);
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn invalid_routes_and_closed_driver_rejections_are_not_sent() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let invalid_broker = owner
        .submit_tracked_produce_to_broker(-1, ProduceRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("negative broker must be rejected"));
    assert!(matches!(
        invalid_broker,
        ProduceSubmitError::InvalidBroker(_)
    ));
    assert_eq!(invalid_broker.delivery(), DeliveryStatus::NotSent);

    let invalid = owner
        .submit_tracked_produce("", 0, ProduceRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("empty topic must be rejected"));
    assert_eq!(invalid.delivery(), DeliveryStatus::NotSent);
    assert_eq!(
        invalid.failure_kind(),
        ProducerAttemptFailureKind::Permanent
    );

    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
    let closed = owner
        .submit_tracked_produce("orders", 0, ProduceRequest::default(), deadline)
        .err()
        .unwrap_or_else(|| panic!("closed driver must reject admission"));
    assert!(matches!(closed, ProduceSubmitError::Driver(_)));
    assert_eq!(closed.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn immediate_driver_rejections_are_normalized_without_retry_policy() {
    let full = ProduceSubmitError::Driver(SubmitError::Full);
    let wake = ProduceSubmitError::Driver(SubmitError::Wake(std::io::Error::other("wake")));
    let closed = ProduceSubmitError::Driver(SubmitError::Closed);
    let invalid_bounds = ProduceSubmitError::Driver(SubmitError::VersionBoundsInvalid {
        api_key: ApiKey::new(0),
        minimum: ApiVersion::new(13),
        maximum: ApiVersion::new(12),
    });

    assert_eq!(
        full.failure_kind(),
        ProducerAttemptFailureKind::LocalCapacity
    );
    assert_eq!(
        wake.failure_kind(),
        ProducerAttemptFailureKind::ConnectionUnavailable
    );
    assert_eq!(closed.failure_kind(), ProducerAttemptFailureKind::Permanent);
    assert_eq!(invalid_bounds.delivery(), DeliveryStatus::NotSent);
    assert_eq!(
        invalid_bounds.failure_kind(),
        ProducerAttemptFailureKind::Permanent
    );
}

fn assert_tracked_produce(_call: &RoutedCall<ProduceResponse>) {}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
