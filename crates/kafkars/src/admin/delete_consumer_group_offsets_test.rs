//! Named consumer-group offset deletion runtime-neutrality scenarios.

use std::future::Future;

use crate::DeleteConsumerGroupOffsets;

#[test]
fn operation_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<DeleteConsumerGroupOffsets>();
}
