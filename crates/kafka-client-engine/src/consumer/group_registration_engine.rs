//! Engine forwarding for bounded classic-group registration and start capture.

use std::{sync::Arc, time::Duration};

use super::{
    GroupConsumerHandle, GroupConsumerRegistration, GroupConsumerRegistrationError,
    GroupConsumerStartCapture, GroupConsumerStartError,
};

impl crate::Engine {
    /// Captures a group-start deadline before facade validation or conversion.
    pub fn capture_group_consumer_start(
        &self,
        timeout: Duration,
    ) -> Result<GroupConsumerStartCapture, GroupConsumerStartError> {
        GroupConsumerStartCapture::capture(self.inner.group_consumer.clone(), timeout)
    }

    /// Registers one bounded classic-group owner without beginning membership.
    #[allow(
        clippy::result_large_err,
        reason = "registration failure preserves the exact caller-owned request for retry or inspection"
    )]
    pub fn register_group_consumer(
        &self,
        registration: GroupConsumerRegistration,
    ) -> Result<GroupConsumerHandle, GroupConsumerRegistrationError> {
        let lifetime: Arc<dyn Send + Sync> = self.inner.clone();
        GroupConsumerHandle::try_register(self.inner.group_consumer.clone(), lifetime, registration)
    }
}
