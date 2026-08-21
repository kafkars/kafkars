//! Named consumer-group offset operation runtime-neutrality scenarios.

use std::future::Future;

use super::ListConsumerGroupOffsets;

#[test]
fn operation_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<ListConsumerGroupOffsets>();
}
