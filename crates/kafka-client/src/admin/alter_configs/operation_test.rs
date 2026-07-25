//! Named incremental configuration operation runtime-neutrality scenarios.

use std::future::Future;

use super::IncrementalAlterConfigs;

#[test]
fn incremental_alter_configs_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<IncrementalAlterConfigs>();
}
