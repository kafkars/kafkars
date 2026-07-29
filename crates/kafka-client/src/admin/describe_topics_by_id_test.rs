//! Public topic-ID operation type guarantees.

use std::future::Future;

use super::DescribeTopicsById;

#[test]
fn topic_id_description_is_a_named_send_future() {
    fn assert_send_future<T: Future + Send>() {}
    assert_send_future::<DescribeTopicsById>();
}
