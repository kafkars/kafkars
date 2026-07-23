//! Cyclic import aliases are unresolved protected evidence.

struct PendingNotificationPermitPool;

use self::Alias as Pool;
use self::Pool as Alias;

fn bypass_joint_budget_owner() {
    let _pool = Alias::from_pending_permit_authority();
}
