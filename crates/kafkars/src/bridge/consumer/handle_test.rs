//! Private bridge claim and close lifecycle scenarios.

use crate::bridge::ClientEngine;
use crate::{ConsumerFetchConfig, ConsumerLimits, ErrorKind, ProducerConfig, Security};

#[test]
fn bridge_claims_once_and_observes_real_close() {
    let engine = ClientEngine::start_with_consumer_fetch(
        vec![String::from("127.0.0.1:1")],
        None,
        Security::plaintext(),
        ProducerConfig::default(),
        None,
        ConsumerFetchConfig::default(),
        ConsumerLimits::default(),
        None,
        None,
    )
    .unwrap_or_else(|error| panic!("start engine: {error}"));
    let mut consumer = engine
        .claim_assigned_consumer()
        .unwrap_or_else(|error| panic!("first claim: {error}"));

    let second = engine.claim_assigned_consumer();
    assert!(matches!(second, Err(error) if error.kind() == ErrorKind::State));

    consumer
        .try_close()
        .unwrap_or_else(|error| panic!("admit assigned-consumer close: {error}"))
        .wait()
        .unwrap_or_else(|error| panic!("close assigned consumer: {error}"));
}
