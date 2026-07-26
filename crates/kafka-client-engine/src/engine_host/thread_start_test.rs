//! Native host-thread acquisition and cancelled-handoff cleanup.

use std::sync::Arc;

use super::{EngineLifecycle, thread_start};

#[test]
fn dropped_startup_sender_closes_the_unowned_host_thread() {
    let lifecycle = Arc::new(EngineLifecycle::new());
    let (sender, handle) = thread_start::start(&lifecycle)
        .unwrap_or_else(|error| panic!("start host thread: {error}"));
    drop(sender);
    handle
        .join()
        .unwrap_or_else(|_panic| panic!("cancelled host thread"));
    assert!(lifecycle.is_closed());
}
