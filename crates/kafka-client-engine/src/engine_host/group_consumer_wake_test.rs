//! The group host join adapts only the domain-neutral embedded-reactor wake.

use std::{sync::Arc, time::Duration};

use crate::{EngineConfig, consumer::GroupConsumerShardWake, driver::DriverOwner};

#[test]
fn group_consumer_requests_the_shared_reactor_turn() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build driver: {error}"));
    let wake = Arc::new(driver.reactor_wake());

    assert!(wake.request_group_turn().is_ok());
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
}
