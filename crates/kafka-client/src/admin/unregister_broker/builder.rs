//! Inert broker-unregistration intent with one destructive submission boundary.

use std::time::Duration;

use crate::bridge::admin::AdminEngine;

use super::UnregisterBroker;

/// Inert request to unregister one nonnegative broker ID from Kafka's metadata quorum.
#[must_use = "call submit to admit the UnregisterBroker operation"]
pub struct UnregisterBrokerBuilder {
    engine: AdminEngine,
    broker_id: i32,
    timeout: Duration,
}

impl UnregisterBrokerBuilder {
    pub(crate) const fn new(engine: AdminEngine, broker_id: i32, timeout: Duration) -> Self {
        Self {
            engine,
            broker_id,
            timeout,
        }
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline, validates the broker ID, and attempts
    /// immediate bounded admission.
    pub fn submit(self) -> UnregisterBroker {
        UnregisterBroker::from_bridge(
            self.engine
                .submit_unregister_broker(self.broker_id, self.timeout),
        )
    }
}

impl std::fmt::Debug for UnregisterBrokerBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UnregisterBrokerBuilder")
            .field("broker_id", &self.broker_id)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
