//! Deliberately UFCS-dispatched notification for allowlist-bypass evidence.

struct PendingNotificationRecovery;

impl PendingNotificationRecovery {
    fn dispatch_all_pending_notifications(self) {}
}

fn bypass_reactor_isolation(recovery: PendingNotificationRecovery) {
    PendingNotificationRecovery::dispatch_all_pending_notifications(recovery);
}
