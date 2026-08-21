//! Static named reassignment operation traits.

use std::future::Future;

use super::ListPartitionReassignments;

#[test]
fn operation_is_a_runtime_neutral_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<ListPartitionReassignments>();
}
