//! Public producer flush type-shape scenarios.

use std::future::Future;

use super::Flush;
use crate::KafkaError;

#[test]
fn named_flush_exposes_async_and_blocking_observation_on_one_type() {
    fn assert_future<T: Future<Output = Result<(), KafkaError>>>() {}
    fn assert_send<T: Send>() {}
    fn assert_wait(_: fn(Flush) -> Result<(), KafkaError>) {}

    assert_future::<Flush>();
    assert_send::<Flush>();
    assert_wait(Flush::wait);
}
