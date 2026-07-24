//! Pre-transfer reclaim rejection preserves the exact application lease.

use super::super::fetch_store::FetchDelivery;
use super::shard::AssignedConsumerShardLockError;

#[must_use = "a rejected reclaim still owns the exact Fetch delivery"]
pub(crate) struct AssignedConsumerReclaimRejection {
    reason: AssignedConsumerShardLockError,
    delivery: FetchDelivery,
}

impl AssignedConsumerReclaimRejection {
    pub(super) const fn new(
        reason: AssignedConsumerShardLockError,
        delivery: FetchDelivery,
    ) -> Self {
        Self { reason, delivery }
    }

    pub(crate) const fn reason(&self) -> AssignedConsumerShardLockError {
        self.reason
    }

    pub(crate) fn into_delivery(self) -> FetchDelivery {
        self.delivery
    }
}
