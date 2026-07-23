//! Crate-private type re-export cannot hide the protected method.

use crate::PrivatePool;

fn bypass() {
    let _pool = PrivatePool::from_pending_permit_authority();
}
