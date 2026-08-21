//! Named consumer-group offset alteration runtime-neutrality scenarios.

use std::future::Future;

use crate::AlterConsumerGroupOffsets;

#[test]
fn operation_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AlterConsumerGroupOffsets>();
}
