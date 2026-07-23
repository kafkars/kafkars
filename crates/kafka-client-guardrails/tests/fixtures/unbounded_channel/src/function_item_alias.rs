//! Deliberately aliased function item for allowlist-bypass evidence.

struct PendingNotificationPermitPool;

impl PendingNotificationPermitPool {
    fn from_pending_permit_authority() -> Self {
        Self
    }
}

fn bypass_joint_budget_owner() {
    let constructor = PendingNotificationPermitPool::from_pending_permit_authority;
    let _pool = constructor();
}
