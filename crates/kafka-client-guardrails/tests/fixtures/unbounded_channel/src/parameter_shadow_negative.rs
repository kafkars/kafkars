//! Parameter shadows end before an outer protected alias is invoked.

mod PendingNotificationPermitPool {
    pub(super) fn from_pending_permit_authority() {}
}

use PendingNotificationPermitPool::from_pending_permit_authority as construct;

fn bypass_after_parameter_scope() {
    let scoped = |construct: fn()| construct();
    scoped(construct);
    construct();
}
