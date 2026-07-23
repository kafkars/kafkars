//! Ownership-aware outcomes from synchronized producer flush admission.

use kafka_client_core::FlushId;

use crate::{ProducerFlushObserver, producer::ProducerHostInvariantError};

use super::{
    super::flush::{AdmittedFlush, FlushAdmissionFailure, FlushRejectionReason},
    ProducerShardWakeError,
    outcome::ProducerPortAcceptedFault,
};

/// Committed flush retaining terminal observation, identity, and execution fault.
#[must_use = "committed flush owns a terminal observer and accepted state"]
pub(crate) struct ProducerPortFlushAccepted {
    observer: ProducerFlushObserver,
    flush_id: Option<FlushId>,
    fault: Result<(), ProducerPortAcceptedFault>,
}

impl ProducerPortFlushAccepted {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ProducerFlushObserver,
        Option<FlushId>,
        Result<(), ProducerPortAcceptedFault>,
    ) {
        (self.observer, self.flush_id, self.fault)
    }

    pub(super) fn with_wake(mut self, wake: Result<(), ProducerShardWakeError>) -> Self {
        if self.fault.is_ok() {
            self.fault = wake.map_err(ProducerPortAcceptedFault::Wake);
        }
        self
    }

    #[cfg(test)]
    pub(crate) fn from_admitted_for_test(admitted: AdmittedFlush) -> Self {
        Self {
            flush_id: Some(admitted.flush_id()),
            observer: admitted.into_flush_observer(),
            fault: Ok(()),
        }
    }
}

impl std::fmt::Debug for ProducerPortFlushAccepted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerPortFlushAccepted")
            .field("observer", &self.observer)
            .field("flush_id", &self.flush_id)
            .field("fault", &self.fault)
            .finish()
    }
}

/// Immediate flush failure before deterministic acceptance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerPortFlushError {
    Contended,
    Rejected(FlushRejectionReason),
    ShardPoisoned,
    HostInvariant(ProducerHostInvariantError),
}

pub(super) fn classify_flush(
    result: Result<AdmittedFlush, FlushAdmissionFailure>,
) -> Result<ProducerPortFlushAccepted, ProducerPortFlushError> {
    match result {
        Ok(admitted) => Ok(ProducerPortFlushAccepted {
            flush_id: Some(admitted.flush_id()),
            observer: admitted.into_flush_observer(),
            fault: Ok(()),
        }),
        Err(FlushAdmissionFailure::Rejected(reason)) => {
            Err(ProducerPortFlushError::Rejected(reason))
        }
        Err(FlushAdmissionFailure::Invariant(error)) => {
            Err(ProducerPortFlushError::HostInvariant(error))
        }
        Err(FlushAdmissionFailure::AcceptedInvariant {
            error,
            flush_id,
            observer,
        }) => Ok(ProducerPortFlushAccepted {
            observer: ProducerFlushObserver::from_completion(observer),
            flush_id,
            fault: Err(ProducerPortAcceptedFault::HostInvariant(error)),
        }),
    }
}
