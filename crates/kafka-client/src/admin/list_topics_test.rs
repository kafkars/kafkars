//! Named all-topic operation runtime-neutrality scenarios.

use std::future::Future;

use super::ListTopics;

#[test]
fn list_topics_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<ListTopics>();
}
