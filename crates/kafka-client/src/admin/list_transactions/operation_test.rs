//! Public operation remains a named non-clone runtime-neutral future.

use std::future::Future;

use super::ListTransactions;

#[test]
fn operation_is_send_and_a_future_without_runtime_dependency() {
    fn assert_send<T: Send>() {}
    fn assert_future<T: Future>() {}
    assert_send::<ListTransactions>();
    assert_future::<ListTransactions>();
}
