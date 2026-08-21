//! Named operation runtime-neutrality scenarios.

use std::future::Future;

use super::CreateTopics;

#[test]
fn create_topics_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<CreateTopics>();
}
