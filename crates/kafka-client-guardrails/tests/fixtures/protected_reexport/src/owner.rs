//! Protected constructor owner.

pub struct PendingNotificationPermitPool;

impl PendingNotificationPermitPool {
    pub fn from_pending_permit_authority() -> Self {
        Self
    }
}

fn construct_for_owner() {
    let _pool = PendingNotificationPermitPool::from_pending_permit_authority();
}
