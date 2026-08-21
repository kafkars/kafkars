//! Named Kafka broker-unregistration operation shape tests.

use std::future::Future;

use super::{UnregisterBroker, UnregisterBrokerResult};

fn assert_future<T: Future<Output = Result<UnregisterBrokerResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<UnregisterBroker>();
}
