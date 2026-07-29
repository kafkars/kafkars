//! Multi-ShareGroup builder type guarantees.

use super::ListShareGroupsOffsetsBuilder;

#[test]
fn builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<ListShareGroupsOffsetsBuilder>();
}
