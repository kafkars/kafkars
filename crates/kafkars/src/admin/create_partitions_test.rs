//! Named partition-increase operation runtime-neutrality scenarios.

use std::future::Future;

use super::CreatePartitions;

#[test]
fn create_partitions_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<CreatePartitions>();
}
