//! Driver-backed producer wake delegation and error-translation scenarios.

use std::{error::Error, io};

use crate::{EngineConfig, producer::ingress::ProducerShardWake};

use super::{
    owner::DriverOwner,
    producer_wake::{ProducerDriverWake, map_wake_failure},
};

#[test]
fn producer_adapter_is_cloneable_thread_safe_and_wakes_the_embedded_reactor() {
    assert_clone_send_sync::<ProducerDriverWake>();
    let owner = owner();
    let wake = owner.producer_wake();

    assert!(wake.wake().is_ok());
    assert!(wake.clone().wake().is_ok());
}

#[test]
fn driver_io_failure_retains_kind_message_and_source() {
    let error = map_wake_failure(io::Error::new(
        io::ErrorKind::ConnectionReset,
        "reactor wake source closed",
    ));

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

fn assert_clone_send_sync<T: Clone + Send + Sync>() {}

fn owner() -> DriverOwner {
    let config = EngineConfig::new(vec!["127.0.0.1:1".to_owned()]);
    DriverOwner::build(&config)
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
