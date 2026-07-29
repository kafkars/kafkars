//! Named producer-fencing operation runtime-neutrality scenarios.

use std::future::Future;

use super::FenceProducers;

#[test]
fn fence_producers_is_a_named_send_future() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<FenceProducers>();
}
