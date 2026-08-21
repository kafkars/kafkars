//! Static public reassignment builder traits.

use super::ListPartitionReassignmentsBuilder;

#[test]
fn builder_is_send_without_claiming_clone_or_sync() {
    fn assert_send<T: Send>() {}
    assert_send::<ListPartitionReassignmentsBuilder>();
}
