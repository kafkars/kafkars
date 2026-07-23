//! `Self` cannot hide a protected constructor inside another implementation.

struct PendingNotificationPermitPool;

impl PendingNotificationPermitPool {
    fn from_pending_permit_authority() -> Self {
        Self
    }

    fn bypass_joint_budget_owner() {
        let _pool = Self::from_pending_permit_authority();
    }
}
