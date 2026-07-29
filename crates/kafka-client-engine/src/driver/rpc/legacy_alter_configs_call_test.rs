//! Linear completion and post-driver recovery scenarios for legacy AlterConfigs.

use std::time::{Duration, Instant};

use kafka_client_core::{
    LegacyAlterConfigsPlan, LegacyAlterConfigsRoute, LegacyConfigEntry,
    LegacyConfigResourceReplacement, LegacyTopicConfigReplacement,
};
use kafka_driver::CompletionError;

use crate::{EngineConfig, driver::DriverOwner};

use super::legacy_alter_configs_call::LegacyAlterConfigsCall;

#[test]
fn completion_fault_retains_any_broker_call_and_exact_ordered_plan() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = LegacyAlterConfigsPlan::new(
        vec![LegacyTopicConfigReplacement::new(
            "orders".to_owned(),
            vec![LegacyConfigEntry::new(
                "cleanup.policy".to_owned(),
                Some("compact".to_owned()),
            )],
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let mut call = LegacyAlterConfigsCall::submit(
        &driver,
        LegacyAlterConfigsRoute::AnyBroker,
        plan.clone(),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error:?}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain route-local ownership"));
    assert!(recovered.matches_correlation(LegacyAlterConfigsRoute::AnyBroker, &plan,));
    recovered.seal();
}

#[test]
fn exact_broker_subplan_enters_the_same_single_destructive_call_owner() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = LegacyAlterConfigsPlan::for_resources(
        vec![
            LegacyConfigResourceReplacement::resource(4, "1".to_owned(), Vec::new()),
            LegacyConfigResourceReplacement::resource(8, "1".to_owned(), Vec::new()),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("generic plan: {error}"));
    let mut call = LegacyAlterConfigsCall::submit(
        &driver,
        LegacyAlterConfigsRoute::ExactBroker(1),
        plan.clone(),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|error| panic!("accepted broker call: {error:?}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain exact-broker ownership"));
    assert!(recovered.matches_correlation(LegacyAlterConfigsRoute::ExactBroker(1), &plan,));
    recovered.seal();
}

#[test]
fn synchronous_rejection_returns_the_exact_route_and_ordered_plan() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = LegacyAlterConfigsPlan::new(
        vec![LegacyTopicConfigReplacement::new(
            "orders".to_owned(),
            vec![LegacyConfigEntry::new("retention.ms".to_owned(), None)],
        )],
        true,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let route = LegacyAlterConfigsRoute::ExactBroker(-1);

    let rejection = match LegacyAlterConfigsCall::submit(
        &driver,
        route,
        plan.clone(),
        Instant::now() + Duration::from_secs(1),
    ) {
        Ok(_call) => panic!("invalid broker ID must reject before driver ownership"),
        Err(rejection) => rejection,
    };

    assert_eq!(rejection.into_correlation(), (route, plan));
}
