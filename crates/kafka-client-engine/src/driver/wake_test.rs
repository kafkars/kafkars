//! Domain-neutral reactor wake and error-retention scenarios.

use std::{error::Error, io};

use crate::EngineConfig;

use super::{
    owner::DriverOwner,
    wake::{ReactorWake, ReactorWakeError},
};

#[test]
fn reactor_wake_is_cloneable_thread_safe_and_coalescible() {
    assert_clone_send_sync::<ReactorWake>();
    let owner = owner();
    let wake = owner.reactor_wake();

    assert!(wake.request().is_ok());
    assert!(wake.clone().request().is_ok());
}

#[test]
fn reactor_wake_failure_retains_kind_message_and_source() {
    let error = ReactorWakeError::from_io_for_test(io::Error::new(
        io::ErrorKind::ConnectionReset,
        "reactor wake source closed",
    ));

    assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    assert_eq!(
        error.to_string(),
        "embedded reactor wake failed: reactor wake source closed"
    );
    assert_eq!(
        error.source().map(ToString::to_string),
        Some("reactor wake source closed".to_owned())
    );
}

fn assert_clone_send_sync<T: Clone + Send + Sync>() {}

fn owner() -> DriverOwner {
    let config = EngineConfig::new(vec!["127.0.0.1:1".to_owned()]);
    DriverOwner::build(&config)
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
