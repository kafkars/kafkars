//! Public renewal builder and operation trait-shape evidence.

use std::future::Future;

use super::{RenewDelegationToken, RenewDelegationTokenBuilder};

#[test]
fn builder_and_operation_are_runtime_neutral_send_values() {
    fn assert_send<T: Send>() {}
    fn assert_future<T: Future>() {}

    assert_send::<RenewDelegationTokenBuilder>();
    assert_send::<RenewDelegationToken>();
    assert_future::<RenewDelegationToken>();
}
