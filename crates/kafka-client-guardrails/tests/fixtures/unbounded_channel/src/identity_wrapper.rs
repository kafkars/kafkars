//! Identity wrappers cannot hide protected function-item references.

struct PendingNotificationPermitPool;

impl PendingNotificationPermitPool {
    fn from_pending_permit_authority() -> Self {
        Self
    }
}

const fn identity<T>(value: T) -> T {
    value
}

fn bypass_joint_budget_owner() {
    let constructor = identity(PendingNotificationPermitPool::from_pending_permit_authority);
    let _pool = constructor();
}
