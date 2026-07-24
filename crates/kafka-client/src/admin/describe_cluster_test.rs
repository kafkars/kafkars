//! Named runtime-neutral `DescribeCluster` operation shape.

use std::future::Future;

use super::DescribeCluster;

#[test]
fn operation_is_a_send_future_without_an_async_runtime() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<DescribeCluster>();
}
