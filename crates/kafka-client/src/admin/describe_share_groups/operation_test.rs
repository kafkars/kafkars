//! Compile-time shape tests for the multi-group ShareGroup operation.

use std::future::Future;

use crate::KafkaError;

use super::{DescribeShareGroups, DescribeShareGroupsResult};

fn assert_send<T: Send>() {}

fn assert_future<T>()
where
    T: Future<Output = Result<DescribeShareGroupsResult, KafkaError>>,
{
}

#[test]
fn operation_is_a_send_named_future_with_a_blocking_wait() {
    assert_send::<DescribeShareGroups>();
    assert_future::<DescribeShareGroups>();

    let _wait: fn(DescribeShareGroups) -> Result<DescribeShareGroupsResult, KafkaError> =
        DescribeShareGroups::wait;
}
