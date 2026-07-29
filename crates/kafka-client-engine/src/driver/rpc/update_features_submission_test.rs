//! Route, deadline, traffic, and conditional-version submission evidence.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{Moment, UpdateFeature, UpdateFeatureIntent, UpdateFeaturesPlan};
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{EngineConfig, clock::MonotonicClock, driver::DriverOwner};

use super::{
    UpdateFeaturesCall,
    update_features_submission::{
        UpdateFeaturesSubmitError, update_features_options, update_features_route,
    },
};

#[test]
fn mutation_uses_the_controller_and_preserves_the_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = update_features_options(deadline, 0)
        .unwrap_or_else(|error| panic!("valid options: {error}"));

    assert_eq!(update_features_route(), Route::Controller);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}

#[test]
fn feature_policy_can_raise_only_the_minimum_to_v1() {
    let options = update_features_options(Instant::now(), 1)
        .unwrap_or_else(|error| panic!("valid options: {error}"));

    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
    assert!(matches!(
        update_features_options(Instant::now(), -1),
        Err(UpdateFeaturesSubmitError::InvalidVersionFloor { actual: -1 })
    ));
    assert!(matches!(
        update_features_options(Instant::now(), 2),
        Err(UpdateFeaturesSubmitError::InvalidVersionFloor { actual: 2 })
    ));
}

#[test]
fn synchronous_rejection_returns_the_exact_plan_and_result_limit() {
    let capture = Arc::new(MonotonicClock::new())
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected_plan = plan(true);
    let failure = match UpdateFeaturesCall::submit(
        &driver,
        expected_plan.clone(),
        4_096,
        capture.operation_deadline(),
        Moment::from_tick(u64::MAX),
    ) {
        Err(failure) => failure,
        Ok(_call) => panic!("elapsed deadline must reject before tracked driver ownership"),
    };
    let (returned_plan, returned_limit) = failure.into_submission_evidence();

    assert_eq!(returned_plan, expected_plan);
    assert_eq!(returned_limit, 4_096);
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let capture = Arc::new(MonotonicClock::new())
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected_plan = plan(false);
    let mut call = UpdateFeaturesCall::submit(
        &driver,
        expected_plan.clone(),
        4_096,
        capture.operation_deadline(),
        capture.now(),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches_evidence(&expected_plan, 4_096));
    assert!(!recovered.matches_evidence(&expected_plan, 4_095));
    recovered.seal();
}

fn plan(validate_only: bool) -> UpdateFeaturesPlan {
    UpdateFeaturesPlan::new(
        vec![UpdateFeature::new(
            "metadata.version".to_owned(),
            7,
            UpdateFeatureIntent::Upgrade,
        )],
        validate_only,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}
