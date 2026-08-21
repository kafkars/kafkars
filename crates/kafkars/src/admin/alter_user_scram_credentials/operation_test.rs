//! Public SCRAM alteration operation shape.

use std::future::Future;

use super::{AlterUserScramCredentials, AlterUserScramCredentialsBuilder};

#[test]
fn operation_and_builder_are_send_without_clone_contracts() {
    fn assert_send<T: Send>() {}
    fn assert_future<T: Future>() {}

    assert_send::<AlterUserScramCredentialsBuilder>();
    assert_send::<AlterUserScramCredentials>();
    assert_future::<AlterUserScramCredentials>();
}
