//! Named operation trait-shape tests.

use std::future::Future;

use crate::{KafkaError, admin::DescribeConsumerGroupsResult};

fn assert_send_future<T>()
where
    T: Future<Output = Result<DescribeConsumerGroupsResult, KafkaError>> + Send,
{
}

#[test]
fn operation_is_a_named_send_future() {
    assert_send_future::<super::DescribeConsumerGroups>();
}
