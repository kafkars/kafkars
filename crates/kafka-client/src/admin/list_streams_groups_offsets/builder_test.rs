//! Multi-Streams-group builder type guarantees.

use super::ListStreamsGroupsOffsetsBuilder;

#[test]
fn builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<ListStreamsGroupsOffsetsBuilder>();
}
