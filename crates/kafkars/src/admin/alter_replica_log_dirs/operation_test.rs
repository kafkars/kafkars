//! Named runtime-neutral operation trait-shape tests.

use std::future::Future;

use crate::KafkaError;

use super::{AlterReplicaLogDirs, AlterReplicaLogDirsResult};

fn assert_send_future<T>()
where
    T: Future<Output = Result<AlterReplicaLogDirsResult, KafkaError>> + Send,
{
}

#[test]
fn operation_is_a_named_send_future() {
    assert_send_future::<AlterReplicaLogDirs>();
}
