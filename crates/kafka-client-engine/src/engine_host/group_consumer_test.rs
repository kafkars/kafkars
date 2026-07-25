//! Private group-registry host scheduling and shutdown-fencing scenarios.

use std::time::Duration;

use kafka_client_core::Moment;

use crate::{EngineConfig, consumer::GroupConsumerRegistry, driver::DriverOwner};

use super::group_consumer::drive_registry;

#[test]
fn one_idle_registry_turn_reports_exact_quiescence() {
    let (mut registry, mut driver) = setup();

    let progress = drive_registry(&mut registry, &driver, false, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("group turn: {error}"));

    assert_eq!(progress.unsettled, 0);
    assert!(!progress.progressed);
    assert_eq!(progress.next_deadline, None);
    shutdown(&mut registry, &mut driver);
}

#[test]
fn shutdown_fences_registry_admission_before_the_bounded_turn() {
    let (mut registry, mut driver) = setup();

    let progress = drive_registry(&mut registry, &driver, true, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("group shutdown turn: {error}"));

    assert_eq!(progress.unsettled, 0);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("shutdown drive must close admission: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group join: {error}"));
}

fn setup() -> (GroupConsumerRegistry, DriverOwner) {
    let registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("group registry: {error}"));
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    (registry, driver)
}

fn shutdown(registry: &mut GroupConsumerRegistry, driver: &mut DriverOwner) {
    registry.close_admission();
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("group recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("group stop: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("group join: {error}"));
}
