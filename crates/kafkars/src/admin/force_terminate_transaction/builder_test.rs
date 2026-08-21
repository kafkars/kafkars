//! Force-termination builder shape scenarios.

use super::ForceTerminateTransactionBuilder;

#[test]
fn builder_is_send_sync_without_runtime_types() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ForceTerminateTransactionBuilder>();
}
