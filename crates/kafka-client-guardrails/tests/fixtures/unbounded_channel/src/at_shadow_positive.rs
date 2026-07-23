//! Every binding in an at-pattern shadows a protected outer alias.

mod PendingNotificationPermitPool {
    pub(super) fn from_pending_permit_authority() {}
}

use PendingNotificationPermitPool::from_pending_permit_authority as construct;

fn harmless() {}

fn invoke_pattern_owned_function() {
    let scoped = |whole @ construct| {
        let _same = whole;
        construct()
    };
    scoped(harmless);
}
