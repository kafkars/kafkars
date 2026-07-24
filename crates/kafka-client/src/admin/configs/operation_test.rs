//! Named topic-configuration operation runtime-neutrality scenarios.

use std::future::Future;

use super::DescribeConfigs;

#[test]
fn describe_configs_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<DescribeConfigs>();
}
