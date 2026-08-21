//! Public shape checks for share-group deletion observation.

use std::future::Future;

use super::DeleteShareGroups;

#[test]
fn operation_is_runtime_neutral_and_thread_transferable() {
    fn assert_future<T: Future>() {}
    fn assert_send_sync_unpin<T: Send + Sync + Unpin>() {}
    assert_future::<DeleteShareGroups>();
    assert_send_sync_unpin::<DeleteShareGroups>();
}
