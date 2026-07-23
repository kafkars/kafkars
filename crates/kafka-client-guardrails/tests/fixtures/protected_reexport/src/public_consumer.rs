//! Public type re-export plus a second local rename cannot hide the method.

use crate::PublicPool as ChainedPool;

fn bypass() {
    let _pool = ChainedPool::from_pending_permit_authority();
}
