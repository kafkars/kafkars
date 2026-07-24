//! Bounded tracked-call ownership scenarios for `IncrementalAlterConfigs`.

use std::time::{Duration, Instant};

use kafka_client_core::{
    ConfigAlteration, IncrementalAlterConfigsInput, IncrementalAlterConfigsPlan, OperationId,
    TopicConfigAlteration,
};

use crate::{EngineConfig, clock::OperationDeadline};

use super::super::DriverOwner;
use super::incremental_alter_configs_calls::{
    IncrementalAlterConfigsAdmissionFailure, IncrementalAlterConfigsCalls,
};

#[test]
fn accepted_call_occupies_the_only_slot_until_driver_shutdown() {
    let mut owner = owner();
    let mut calls = IncrementalAlterConfigsCalls::new(1);
    let permit = calls
        .try_reserve()
        .unwrap_or_else(|| panic!("one tracked slot must be available"));
    permit
        .submit(
            &owner,
            OperationId::from_raw(1),
            OperationDeadline::from_parts_for_test(
                kafka_client_core::Deadline::from_tick(u64::MAX),
                Instant::now() + Duration::from_secs(1),
            ),
            plan(),
            256 * 1024,
        )
        .unwrap_or_else(|error| panic!("submit tracked IncrementalAlterConfigs call: {error:?}"));
    assert_eq!(calls.retained_count(), 1);
    assert!(calls.try_reserve().is_none());
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
    calls.discard_after_driver_shutdown();
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn zero_capacity_rejects_before_submission() {
    let mut calls = IncrementalAlterConfigsCalls::new(0);
    assert!(calls.try_reserve().is_none());
}

#[test]
fn driver_admission_failure_is_definitely_unsent_core_rejection() {
    assert_eq!(
        IncrementalAlterConfigsAdmissionFailure::Driver.into_core_input(),
        IncrementalAlterConfigsInput::DriverRejected
    );
}

fn plan() -> IncrementalAlterConfigsPlan {
    IncrementalAlterConfigsPlan::new(
        vec![TopicConfigAlteration::new(
            "orders".to_owned(),
            vec![ConfigAlteration::delete("retention.ms".to_owned())],
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("valid IncrementalAlterConfigs plan: {error}"))
}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
