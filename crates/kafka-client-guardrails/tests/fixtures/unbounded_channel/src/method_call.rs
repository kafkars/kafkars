//! Inferred receiver types cannot hide a forbidden recovery dispatch.

struct Recovery;

impl Recovery {
    fn dispatch_all_pending_notifications(&self) {}

    fn publish_port(&self) {}
}

fn bypass_path_detection(recovery: &Recovery) {
    recovery.dispatch_all_pending_notifications();
    recovery.publish_port();
}
