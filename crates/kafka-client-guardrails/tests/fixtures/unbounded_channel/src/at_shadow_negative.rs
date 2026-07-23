//! At-pattern shadows end before an outer protected alias is referenced.

mod PendingNotificationPermitPool {
    pub(super) fn from_pending_permit_authority() {}
}

use PendingNotificationPermitPool::from_pending_permit_authority as construct;

fn harmless() {}

fn bypass_after_pattern_scope() {
    let scoped = |whole @ construct| {
        let _same = whole;
        construct()
    };
    scoped(harmless);
    construct();
}
