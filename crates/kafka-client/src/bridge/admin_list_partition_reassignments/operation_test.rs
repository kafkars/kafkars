//! Static private operation ownership tests.

use std::future::Future;

use super::operation::AdminListPartitionReassignments;

#[test]
fn private_operation_is_runtime_neutral_and_sendable() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AdminListPartitionReassignments>();
}
