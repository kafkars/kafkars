//! Named runtime-neutral ACL description operation shape tests.

use std::future::Future;

use crate::KafkaError;

use super::{DescribeAcls, DescribeAclsResult};

fn assert_send_future<T>()
where
    T: Future<Output = Result<DescribeAclsResult, KafkaError>> + Send,
{
}

#[test]
fn operation_is_a_named_send_future() {
    assert_send_future::<DescribeAcls>();
}
