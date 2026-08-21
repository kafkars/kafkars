//! Named partition-transaction abort operation shape tests.

use std::future::Future;

use super::AbortPartitionTransaction;

fn assert_future<T: Future<Output = Result<(), crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<AbortPartitionTransaction>();
}
