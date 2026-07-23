//! Shared driver-shutdown subscription ownership scenarios.

use std::time::Duration;

use crate::EngineConfig;

use super::DriverOwner;

#[test]
fn repeated_close_reuses_one_shared_shutdown_subscription() {
    let mut owner = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"));

    owner
        .close_admission()
        .unwrap_or_else(|error| panic!("first explicit shutdown request: {error}"));
    owner
        .close_admission()
        .unwrap_or_else(|error| panic!("second explicit shutdown request: {error}"));

    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("drive shared shutdown barrier: {error}"));
    assert!(owner.is_shutdown());
}
