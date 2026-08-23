//! Engine forwarding for capture-first share-member construction.

use std::{sync::Arc, time::Duration};

use super::{
    ShareConsumerPort,
    public_registration::{
        ShareConsumerHandle, ShareConsumerRegistration, ShareConsumerStartCapture,
    },
    public_registration_error::{
        ShareConsumerRegistrationError, ShareConsumerRegistrationErrorKind,
    },
};

impl crate::Engine {
    /// Captures the membership deadline before facade validation or conversion.
    pub fn capture_share_consumer_start(
        &self,
        timeout: Duration,
    ) -> Result<ShareConsumerStartCapture, ShareConsumerRegistrationErrorKind> {
        ShareConsumerStartCapture::capture(self.inner.share_consumer.clone(), timeout)
    }

    /// Registers and starts one bounded share member under the captured deadline.
    pub fn register_share_consumer(
        &self,
        capture: ShareConsumerStartCapture,
        registration: ShareConsumerRegistration,
    ) -> Result<ShareConsumerHandle, ShareConsumerRegistrationError> {
        let lifetime: Arc<dyn Send + Sync> = self.inner.clone();
        ShareConsumerHandle::try_register_started(
            self.inner.share_consumer.clone(),
            lifetime,
            capture,
            registration,
        )
    }
}

impl ShareConsumerPort {
    pub(super) fn shares_registry_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}
