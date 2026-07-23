//! Chained scoped imports cannot hide a protected constructor invocation.

mod owner {
    pub(super) struct PendingNotificationPermitPool;

    impl PendingNotificationPermitPool {
        pub(super) fn from_pending_permit_authority() -> Self {
            Self
        }
    }
}

use owner::PendingNotificationPermitPool as Pool;
use self::Pool as Alias;

fn bypass_joint_budget_owner(Pool: fn()) {
    let _shadow = Pool;
    let _pool = Alias::from_pending_permit_authority();
}
