//! Tracked `InitProducerId` submission scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::InitProducerIdResponse;

use crate::{EngineConfig, protocol::init_producer_id::nontransactional_init_producer_id_request};

use super::{super::DriverOwner, init_producer_id_submission::init_producer_id_options};

#[test]
fn options_preserve_original_deadline_control_lane_and_stable_v5_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = init_producer_id_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Control);
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(5)));
}

#[test]
fn accepted_any_broker_submission_returns_a_tracked_typed_call() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(1);
    let call = owner
        .submit_tracked_init_producer_id(nontransactional_init_producer_id_request(), deadline)
        .unwrap_or_else(|error| panic!("tracked identity admission: {error}"));

    assert!(call.try_result().is_none());
    assert_tracked_identity(&call);
    drop(call);
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn closed_driver_rejection_remains_an_immediate_submit_error() {
    let mut owner = owner();
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));

    let rejection = owner
        .submit_tracked_init_producer_id(
            nontransactional_init_producer_id_request(),
            Instant::now() + Duration::from_secs(1),
        )
        .err()
        .unwrap_or_else(|| panic!("closed driver must reject identity admission"));
    assert!(matches!(rejection, SubmitError::Closed));
}

fn assert_tracked_identity(_call: &RoutedCall<InitProducerIdResponse>) {}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
