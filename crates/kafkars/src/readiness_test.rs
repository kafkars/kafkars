//! Tests for the public runtime-neutral readiness observer shape.

use std::future::Future;

use crate::{KafkaError, Ready};

#[test]
fn readiness_is_a_named_send_future_without_an_async_runtime() {
    fn assert_future<T: Future<Output = Result<(), KafkaError>> + Send>() {}
    assert_future::<Ready>();
}
