//! Static private reassignment operation ownership tests.

use std::future::Future;

use super::alter_operation::AdminAlterPartitionReassignments;

#[test]
fn private_operation_is_runtime_neutral_and_sendable() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AdminAlterPartitionReassignments>();
}
