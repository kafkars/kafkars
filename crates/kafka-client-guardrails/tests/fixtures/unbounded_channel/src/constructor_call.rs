//! Deliberately forbidden constructor call for complete-allowlist evidence.

struct PendingNotificationPermitPool;

impl PendingNotificationPermitPool {
    fn from_pending_permit_authority() -> Self {
        Self
    }
}

fn bypass_joint_budget_owner() {
    let _pool = PendingNotificationPermitPool::from_pending_permit_authority();
}
