//! Public multi-consumer-group observer type guarantees.

use std::future::Future;

use super::ListConsumerGroupsOffsets;

#[test]
fn multi_group_offset_listing_is_a_named_send_future() {
    fn assert_send_future<T: Future + Send>() {}
    assert_send_future::<ListConsumerGroupsOffsets>();
}
