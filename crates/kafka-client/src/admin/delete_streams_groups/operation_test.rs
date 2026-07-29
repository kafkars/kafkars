//! Public shape checks for streams-group deletion observation.

use std::future::Future;

use super::DeleteStreamsGroups;

#[test]
fn operation_is_runtime_neutral_and_thread_transferable() {
    fn assert_future<T: Future>() {}
    fn assert_send_sync_unpin<T: Send + Sync + Unpin>() {}
    assert_future::<DeleteStreamsGroups>();
    assert_send_sync_unpin::<DeleteStreamsGroups>();
}
