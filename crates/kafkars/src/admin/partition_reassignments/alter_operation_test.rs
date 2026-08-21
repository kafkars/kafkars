//! Named reassignment operation Future evidence.

use std::future::Future;

use super::AlterPartitionReassignments;

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    fn assert_future<T: Future>() {}
    assert_future::<AlterPartitionReassignments>();
}
