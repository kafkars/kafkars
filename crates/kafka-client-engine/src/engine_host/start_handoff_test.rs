//! Startup rollback joins the native thread without exposing its token.

use std::thread;

use super::start_handoff::join_cancelled;

#[test]
fn cancelled_startup_joins_the_host_thread() {
    let handle = thread::spawn(|| {});

    join_cancelled(handle);
}
