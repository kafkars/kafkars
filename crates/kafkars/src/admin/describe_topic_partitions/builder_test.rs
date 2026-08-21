//! Inert one-page builder and named-operation surface tests.

use std::{future::Future, time::Duration};

use super::{
    DescribeTopicPartitions, DescribeTopicPartitionsBuilder, DescribeTopicPartitionsCursor,
    DescribeTopicPartitionsPage,
};

fn assert_future<T: Future<Output = Result<DescribeTopicPartitionsPage, crate::KafkaError>>>() {}

#[test]
fn operation_is_one_named_runtime_neutral_future() {
    assert_future::<DescribeTopicPartitions>();
}

#[test]
fn builder_controls_remain_inert_until_the_explicit_submit_boundary() {
    let limit: fn(DescribeTopicPartitionsBuilder, u32) -> DescribeTopicPartitionsBuilder =
        DescribeTopicPartitionsBuilder::response_partition_limit;
    let cursor: fn(
        DescribeTopicPartitionsBuilder,
        DescribeTopicPartitionsCursor,
    ) -> DescribeTopicPartitionsBuilder = DescribeTopicPartitionsBuilder::cursor;
    let deadline: fn(DescribeTopicPartitionsBuilder, Duration) -> DescribeTopicPartitionsBuilder =
        DescribeTopicPartitionsBuilder::deadline_after;
    let submit: fn(DescribeTopicPartitionsBuilder) -> DescribeTopicPartitions =
        DescribeTopicPartitionsBuilder::submit;

    let _ = (limit, cursor, deadline, submit);
}

#[test]
fn builder_and_operation_are_send_without_an_async_runtime() {
    fn assert_send<T: Send>() {}
    assert_send::<DescribeTopicPartitionsBuilder>();
    assert_send::<DescribeTopicPartitions>();
}
