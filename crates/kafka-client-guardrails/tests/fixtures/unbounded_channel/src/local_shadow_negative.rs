//! Local shadows end before an outer protected alias is invoked.

mod PendingNotificationPermitPool {
    pub(super) fn from_pending_permit_authority() {}
}

use PendingNotificationPermitPool::from_pending_permit_authority as construct;

fn harmless() {}

fn bypass_after_local_scope() {
    {
        let construct = || harmless();
        construct();
    }
    construct();
}
