//! First-slice assigned-consumer startup owns one real bounded lifecycle.

use std::{sync::Arc, time::Duration};

use crate::{EngineConfig, clock::MonotonicClock, driver::DriverOwner};

use super::assigned_consumer_start::start_assigned_consumer;

#[test]
fn startup_constructs_one_idle_owner_and_one_nonclone_port() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build driver: {error}"));
    let clock = Arc::new(MonotonicClock::new());
    let (owner, _port) = start_assigned_consumer(clock, Arc::new(driver.reactor_wake()))
        .unwrap_or_else(|error| panic!("assigned consumer: {error:?}"));

    assert_eq!(
        owner
            .try_with_owner(|assigned| assigned.unsettled())
            .unwrap_or_else(|error| panic!("owner slot: {error:?}")),
        0
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
}
