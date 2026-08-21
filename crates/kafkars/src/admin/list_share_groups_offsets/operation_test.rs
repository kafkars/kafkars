//! Multi-ShareGroup operation type guarantees.

use std::future::Future;

use super::ListShareGroupsOffsets;

#[test]
fn operation_is_a_named_send_future() {
    fn assert_send_future<T: Future + Send>() {}
    assert_send_future::<ListShareGroupsOffsets>();
}
