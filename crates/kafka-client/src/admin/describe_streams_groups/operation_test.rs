//! Compile-time shape tests for the multi-group `StreamsGroup` operation.

use std::future::Future;

use crate::KafkaError;

use super::{DescribeStreamsGroups, DescribeStreamsGroupsResult};

fn assert_send<T: Send>() {}

fn assert_future<T>()
where
    T: Future<Output = Result<DescribeStreamsGroupsResult, KafkaError>>,
{
}

#[test]
fn operation_is_a_send_named_future_with_a_blocking_wait() {
    assert_send::<DescribeStreamsGroups>();
    assert_future::<DescribeStreamsGroups>();

    let wait: fn(DescribeStreamsGroups) -> Result<DescribeStreamsGroupsResult, KafkaError> =
        DescribeStreamsGroups::wait;
    let _ = wait;
}
