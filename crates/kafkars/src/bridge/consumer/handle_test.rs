//! Private bridge claim and close lifecycle scenarios.

use std::time::{Duration, Instant};

use crate::bridge::ClientEngine;
use crate::{
    ConsumerFetchConfig, ConsumerLimits, ErrorKind, ProducerConfig, RetryAdvice, Security,
};

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

    let admission_deadline = Instant::now() + Duration::from_secs(2);
    let close = loop {
        match consumer.try_close() {
            Ok(close) => break close,
            Err(error)
                if error.retry_advice() == RetryAdvice::RetrySafe
                    && Instant::now() < admission_deadline =>
            {
                std::thread::yield_now();
            }
            Err(error) => panic!("admit assigned-consumer close: {error}"),
        }
    };
    close
        .wait()
        .unwrap_or_else(|error| panic!("close assigned consumer: {error}"));
}
