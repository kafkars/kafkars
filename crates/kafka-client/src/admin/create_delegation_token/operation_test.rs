//! Public delegation-token creation operation shape.

use std::future::Future;

use super::{CreateDelegationToken, CreateDelegationTokenBuilder};

#[test]
fn operation_and_builder_are_send_without_clone_contracts() {
    fn assert_send<T: Send>() {}
    fn assert_future<T: Future>() {}

    assert_send::<CreateDelegationTokenBuilder>();
    assert_send::<CreateDelegationToken>();
    assert_future::<CreateDelegationToken>();
}
