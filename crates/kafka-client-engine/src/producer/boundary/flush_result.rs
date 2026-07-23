//! Stable accepted producer flush ownership.

use std::fmt;

use super::super::ingress::ProducerPortFlushAccepted;
use super::result::ProducerAcceptedFault;
use crate::ProducerFlushObserver;

/// Successful flush ownership transfer and its sole terminal observer.
#[must_use = "accepted producer flushes must retain or abandon their observer"]
pub struct ProducerTryFlushAccepted {
    observer: ProducerFlushObserver,
    fault: Option<ProducerAcceptedFault>,
}

impl ProducerTryFlushAccepted {
    /// Returns a post-ownership execution fault without revoking admission.
    pub const fn fault(&self) -> Option<&ProducerAcceptedFault> {
        self.fault.as_ref()
    }

    /// Transfers the sole terminal observer to the caller.
    pub fn into_observer(self) -> ProducerFlushObserver {
        self.observer
    }

    pub(super) fn from_port(accepted: ProducerPortFlushAccepted) -> Self {
        let (observer, _flush_id, fault) = accepted.into_parts();
        Self {
            observer,
            fault: fault.err().map(ProducerAcceptedFault::from_port),
        }
    }
}

impl fmt::Debug for ProducerTryFlushAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerTryFlushAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
