//! Inert `DescribeProducers` builder and named-operation surface tests.

use std::{future::Future, time::Duration};

use super::{DescribeProducers, DescribeProducersBuilder, DescribeProducersResult};

fn assert_future<T: Future<Output = Result<DescribeProducersResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<DescribeProducers>();
}

#[test]
fn builder_surface_keeps_timeout_configuration_inert_until_submit() {
    let broker_id: fn(DescribeProducersBuilder, i32) -> DescribeProducersBuilder =
        DescribeProducersBuilder::broker_id;
    let deadline: fn(DescribeProducersBuilder, Duration) -> DescribeProducersBuilder =
        DescribeProducersBuilder::deadline_after;
    let submit: fn(DescribeProducersBuilder) -> DescribeProducers =
        DescribeProducersBuilder::submit;

    let _ = (broker_id, deadline, submit);
}

#[test]
fn named_operation_has_a_stable_debug_identity() {
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_debug::<DescribeProducers>();
}
