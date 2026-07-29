//! Inert DescribeTransactions builder and named-operation surface tests.

use std::{future::Future, time::Duration};

use super::{DescribeTransactions, DescribeTransactionsBuilder, DescribeTransactionsResult};

fn assert_future<T: Future<Output = Result<DescribeTransactionsResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<DescribeTransactions>();
}

#[test]
fn builder_surface_keeps_timeout_configuration_inert_until_submit() {
    let deadline: fn(DescribeTransactionsBuilder, Duration) -> DescribeTransactionsBuilder =
        DescribeTransactionsBuilder::deadline_after;
    let submit: fn(DescribeTransactionsBuilder) -> DescribeTransactions =
        DescribeTransactionsBuilder::submit;

    let _ = (deadline, submit);
}

#[test]
fn named_operation_has_a_stable_debug_identity() {
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_debug::<DescribeTransactions>();
}
