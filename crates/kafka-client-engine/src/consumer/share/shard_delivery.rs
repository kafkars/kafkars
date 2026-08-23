//! Blocking return of one external share delivery to its exact registry owner.

use super::shard::ShareConsumerShardState;

impl ShareConsumerShardState {
    pub(super) fn return_delivery_blocking(
        &self,
        delivery: super::fetch_delivery::ShareFetchDelivery,
    ) {
        let mut registry = self.control_registry();
        let returned_to_owner = registry.reclaim_delivery(delivery).is_ok();
        drop(registry);
        if returned_to_owner {
            let _wake_result = self.request_turn();
        }
    }
}
