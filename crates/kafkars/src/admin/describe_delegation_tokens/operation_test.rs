//! Public operation trait-shape evidence.

use std::future::Future;

use super::{DescribeDelegationTokens, DescribeDelegationTokensBuilder};

#[test]
fn builder_and_operation_are_runtime_neutral_send_values() {
    fn assert_send<T: Send>() {}
    fn assert_future<T: Future>() {}

    assert_send::<DescribeDelegationTokensBuilder>();
    assert_send::<DescribeDelegationTokens>();
    assert_future::<DescribeDelegationTokens>();
}
