//! Stable ownership-aware results for immediate producer admission.

use std::fmt;

use super::super::ingress::{ProducerPortAccepted, ProducerPortAcceptedFault};
use crate::ProducerDeliveryObserver;

/// Successful ownership transfer and its sole terminal observer.
#[must_use = "accepted producer work must retain or deliberately abandon its observer"]
pub struct ProducerTrySendAccepted {
    observer: ProducerDeliveryObserver,
    fault: Option<ProducerAcceptedFault>,
}

impl ProducerTrySendAccepted {
    /// Returns a post-ownership execution fault without revoking admission.
    pub const fn fault(&self) -> Option<&ProducerAcceptedFault> {
        self.fault.as_ref()
    }

    /// Transfers the sole terminal observer to the caller.
    pub fn into_observer(self) -> ProducerDeliveryObserver {
        self.observer
    }

    pub(super) fn from_port(accepted: ProducerPortAccepted) -> Self {
        let (observer, _operation_id, fault) = accepted.into_parts();
        Self {
            observer,
            fault: fault.err().map(ProducerAcceptedFault::from_port),
        }
    }
}

impl fmt::Debug for ProducerTrySendAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerTrySendAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}

/// Kind of post-ownership execution fault retained with accepted work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerAcceptedFaultKind {
    /// The synchronized host detected an internal state disagreement.
    HostInvariant,
    /// Admission committed, but requesting a host turn failed.
    Wake,
}

/// Post-ownership fault that cannot be translated into record return.
#[derive(Debug)]
pub struct ProducerAcceptedFault {
    kind: ProducerAcceptedFaultKind,
    detail: String,
}

impl ProducerAcceptedFault {
    /// Returns the stable fault category.
    pub const fn kind(&self) -> ProducerAcceptedFaultKind {
        self.kind
    }

    pub(super) fn from_port(fault: ProducerPortAcceptedFault) -> Self {
        match fault {
            ProducerPortAcceptedFault::HostInvariant(error) => Self {
                kind: ProducerAcceptedFaultKind::HostInvariant,
                detail: error.to_string(),
            },
            ProducerPortAcceptedFault::Wake(error) => Self {
                kind: ProducerAcceptedFaultKind::Wake,
                detail: error.to_string(),
            },
        }
    }
}

impl fmt::Display for ProducerAcceptedFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for ProducerAcceptedFault {}
