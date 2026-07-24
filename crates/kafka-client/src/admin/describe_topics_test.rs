//! Named topic-description operation runtime-neutrality scenarios.

use std::future::Future;

use super::DescribeTopics;

#[test]
fn describe_topics_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<DescribeTopics>();
}
