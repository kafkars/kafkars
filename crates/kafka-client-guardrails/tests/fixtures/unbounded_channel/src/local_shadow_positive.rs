//! A non-path local binding prevents an outer protected alias from leaking inward.

mod PendingNotificationPermitPool {
    pub(super) fn from_pending_permit_authority() {}
}

use PendingNotificationPermitPool::from_pending_permit_authority as construct;

fn harmless() {}

fn invoke_local_function() {
    let construct = || harmless();
    construct();
}
