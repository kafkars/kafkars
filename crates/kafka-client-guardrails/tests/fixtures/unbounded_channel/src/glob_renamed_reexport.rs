//! A glob re-export followed by a rename cannot hide a protected reference.

mod owner {
    pub(super) struct PendingNotificationPermitPool;

    impl PendingNotificationPermitPool {
        pub(super) fn from_pending_permit_authority() -> Self {
            Self
        }
    }
}

mod alias {
    pub use super::owner::*;
}

use alias::PendingNotificationPermitPool as Pool;

fn bypass_joint_budget_owner() {
    let constructor = Pool::from_pending_permit_authority;
    let _pool = constructor();
}
