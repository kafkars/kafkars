//! Shared bridge shutdown worker and retained-terminal scenarios.

use std::thread;

use super::client::ClientEngine;
use crate::{ConsumerFetchConfig, ConsumerLimits, ProducerConfig, Security};

#[test]
fn concurrent_bridge_observers_receive_one_retained_shutdown_report() {
    let client = ClientEngine::start_with_consumer_fetch(
        vec!["127.0.0.1:1".to_owned()],
        None,
        Security::plaintext(),
        ProducerConfig::default(),
        None,
        ConsumerFetchConfig::default(),
        ConsumerLimits::default(),
    )
    .unwrap_or_else(|error| panic!("start client bridge: {error}"));
    let first = client.shutdown();
    let second = client.shutdown();
    let waiter = thread::spawn(move || first.wait());

    assert!(second.wait().is_ok());
    assert!(waiter.join().is_ok_and(|result| result.is_ok()));
    assert!(client.shutdown().wait().is_ok());
}
