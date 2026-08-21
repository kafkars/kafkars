//! Named deletion operation runtime-neutrality scenarios.

use std::future::Future;

use super::DeleteTopics;

#[test]
fn delete_topics_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<DeleteTopics>();
}
