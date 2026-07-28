//! Named runtime-neutral ACL creation operation shape tests.

use std::future::Future;

use crate::KafkaError;

use super::{CreateAcls, CreateAclsResult};

fn assert_send_future<T>()
where
    T: Future<Output = Result<CreateAclsResult, KafkaError>> + Send,
{
}

#[test]
fn operation_is_a_named_send_future() {
    assert_send_future::<CreateAcls>();
}
