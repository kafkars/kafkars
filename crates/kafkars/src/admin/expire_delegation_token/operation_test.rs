//! Public expiration builder and operation trait-shape evidence.

use std::future::Future;

use super::{ExpireDelegationToken, ExpireDelegationTokenBuilder};

#[test]
fn builder_and_operation_are_runtime_neutral_send_values() {
    fn assert_send<T: Send>() {}
    fn assert_future<T: Future>() {}

    assert_send::<ExpireDelegationTokenBuilder>();
    assert_send::<ExpireDelegationToken>();
    assert_future::<ExpireDelegationToken>();
}
