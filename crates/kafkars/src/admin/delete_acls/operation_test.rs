//! Named runtime-neutral ACL deletion operation shape tests.

use std::future::Future;

use crate::KafkaError;

use super::{DeleteAcls, DeleteAclsResult};

fn assert_send_future<T>()
where
    T: Future<Output = Result<DeleteAclsResult, KafkaError>> + Send,
{
}

#[test]
fn operation_is_a_named_send_future() {
    assert_send_future::<DeleteAcls>();
}
