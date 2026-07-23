//! A parameter binding prevents an outer protected alias from leaking inward.

mod PendingNotificationPermitPool {
    pub(super) fn from_pending_permit_authority() {}
}

use PendingNotificationPermitPool::from_pending_permit_authority as construct;

fn invoke_caller_owned_function(construct: fn()) {
    construct();
}
