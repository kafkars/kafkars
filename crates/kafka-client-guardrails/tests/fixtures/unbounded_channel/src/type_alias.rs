//! Type aliases cannot hide a protected constructor invocation.

mod owner {
    pub(super) struct PendingNotificationPermitPool;

    impl PendingNotificationPermitPool {
        pub(super) fn from_pending_permit_authority() -> Self {
            Self
        }
    }
}

type Pool = owner::PendingNotificationPermitPool;

fn bypass_joint_budget_owner() {
    let _pool = Pool::from_pending_permit_authority();
}
