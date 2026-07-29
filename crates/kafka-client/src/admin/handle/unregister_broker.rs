//! Kafka broker-unregistration entry point on the shared admin handle.

use super::Admin;
use crate::admin::UnregisterBrokerBuilder;

impl Admin {
    /// Builds inert intent to unregister one nonnegative broker ID from Kafka's
    /// metadata quorum.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`UnregisterBrokerBuilder::submit`] is called. A negative broker ID is
    /// rejected definitely unsent at that boundary.
    pub fn unregister_broker(&self, broker_id: i32) -> UnregisterBrokerBuilder {
        UnregisterBrokerBuilder::new(
            self.engine.clone(),
            broker_id,
            self.engine.default_timeout(),
        )
    }
}
