//! Topic-ID DeleteTopics operation type guarantees.

use std::future::Future;

use super::DeleteTopicsById;

#[test]
fn topic_id_deletion_is_a_named_send_future() {
    fn assert_send_future<T: Future + Send>() {}
    assert_send_future::<DeleteTopicsById>();
}
