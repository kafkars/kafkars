//! Generic incremental configuration operation type scenarios.

use std::future::Future;

use super::IncrementalAlterConfigResources;

#[test]
fn generic_operation_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<IncrementalAlterConfigResources>();
}
