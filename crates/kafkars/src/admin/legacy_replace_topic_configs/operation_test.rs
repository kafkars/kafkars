//! Named legacy replacement operation runtime-neutrality scenarios.

use std::future::Future;

use super::LegacyReplaceTopicConfigs;

#[test]
fn legacy_replace_topic_configs_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<LegacyReplaceTopicConfigs>();
}
