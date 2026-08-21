//! General group listing operation remains runtime-neutral.

use std::future::Future;

use super::ListGroups;

#[test]
fn operation_is_send_and_a_future_without_runtime_dependency() {
    fn assert_send<T: Send>() {}
    fn assert_future<T: Future>() {}
    assert_send::<ListGroups>();
    assert_future::<ListGroups>();
}
