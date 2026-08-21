//! Public operation remains a named runtime-neutral future.

use std::future::Future;

use super::ListConsumerGroups;

#[test]
fn operation_is_send_and_a_future_without_runtime_dependency() {
    fn assert_send<T: Send>() {}
    fn assert_future<T: Future>() {}
    assert_send::<ListConsumerGroups>();
    assert_future::<ListConsumerGroups>();
}
