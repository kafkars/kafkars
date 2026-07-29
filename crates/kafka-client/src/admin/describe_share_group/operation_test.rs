//! Named ShareGroup description operation ownership tests.

use std::future::Future;

use super::DescribeShareGroup;

#[test]
fn named_operation_is_a_send_runtime_neutral_future() {
    fn assert_future<T: Future + Send + std::fmt::Debug>() {}
    assert_future::<DescribeShareGroup>();
}
