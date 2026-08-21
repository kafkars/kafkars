//! Named Streams-group offset observer runtime-neutrality evidence.

use std::future::Future;

use super::ListStreamsGroupOffsets;

#[test]
fn operation_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<ListStreamsGroupOffsets>();
}
