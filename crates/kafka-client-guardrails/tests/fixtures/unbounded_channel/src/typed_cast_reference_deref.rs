//! Typed, cast, referenced, and dereferenced function items remain protected.

struct PendingNotificationPermitPool;

impl PendingNotificationPermitPool {
    fn from_pending_permit_authority() -> Self {
        Self
    }
}

type Constructor = fn() -> PendingNotificationPermitPool;

fn bypass_joint_budget_owner() {
    let constructor: Constructor = PendingNotificationPermitPool::from_pending_permit_authority as Constructor;
    let constructor_reference = &constructor;
    let _pool = (*constructor_reference)();
}
