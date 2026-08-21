//! Multi-Streams-group observer type guarantees.

use std::future::Future;

use super::ListStreamsGroupsOffsets;

#[test]
fn operation_is_a_named_send_future() {
    fn assert_send_future<T: Future + Send>() {}
    assert_send_future::<ListStreamsGroupsOffsets>();
}
