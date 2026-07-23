//! Concrete producer adaptation and error-identity scenarios.

use std::{error::Error, io};

use crate::{
    EngineConfig,
    driver::{DriverOwner, ReactorWakeError},
};

use super::{ProducerShardWake, ProducerShardWakeError};

#[test]
fn producer_adapter_wakes_the_shared_embedded_reactor() {
    let config = EngineConfig::new(vec!["127.0.0.1:1".to_owned()]);
    let owner = DriverOwner::build(&config)
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"));
    let wake = owner.reactor_wake();

    assert!(ProducerShardWake::wake(&wake).is_ok());
}

#[test]
fn producer_adapter_preserves_the_original_io_error() {
    let error = ProducerShardWakeError::from(ReactorWakeError::from_io_for_test(io::Error::new(
        io::ErrorKind::ConnectionReset,
        "reactor wake source closed",
    )));

    assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    assert_eq!(
        error.to_string(),
        "producer shard wake failed: reactor wake source closed"
    );
    assert_eq!(
        error.source().map(ToString::to_string),
        Some("reactor wake source closed".to_owned())
    );
}
