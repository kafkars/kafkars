//! Public selected-replica builder and result thread-safety shape tests.

use super::{DescribeReplicaLogDirsBuilder, DescribeReplicaLogDirsResult};

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn builder_and_result_are_send_sync_without_runtime_types() {
    assert_send_sync::<DescribeReplicaLogDirsBuilder>();
    assert_send_sync::<DescribeReplicaLogDirsResult>();
}
