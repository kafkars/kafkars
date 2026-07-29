//! Named classic-group operation trait-shape tests.

use std::future::Future;

use super::{DescribeClassicGroups, DescribeClassicGroupsResult};
use crate::KafkaError;

#[test]
fn operation_is_one_named_runtime_neutral_send_future() {
    fn assert_operation<T>()
    where
        T: Future<Output = Result<DescribeClassicGroupsResult, KafkaError>>
            + Send
            + std::fmt::Debug,
    {
    }

    assert_operation::<DescribeClassicGroups>();
}
