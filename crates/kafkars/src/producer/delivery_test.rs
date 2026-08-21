//! Public delivery type-shape scenarios.

use std::future::Future;

use super::Delivery;
use crate::{KafkaError, RecordMetadata};

#[test]
fn named_delivery_exposes_async_and_blocking_observation_on_one_type() {
    fn assert_future<T: Future<Output = Result<RecordMetadata, KafkaError>>>() {}
    fn assert_wait(_: fn(Delivery) -> Result<RecordMetadata, KafkaError>) {}

    assert_future::<Delivery>();
    assert_wait(Delivery::wait);
}
