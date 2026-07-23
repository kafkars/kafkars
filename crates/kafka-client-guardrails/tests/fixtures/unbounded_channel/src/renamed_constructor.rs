//! Deliberately renamed constructor import for allowlist-bypass evidence.

mod owner {
    pub(super) struct PendingNotificationPermitPool;

    impl PendingNotificationPermitPool {
        pub(super) fn from_pending_permit_authority() -> Self {
            Self
        }
    }
}

use owner::PendingNotificationPermitPool as Pool;

fn bypass_joint_budget_owner() {
    let _pool = Pool::from_pending_permit_authority();
}
