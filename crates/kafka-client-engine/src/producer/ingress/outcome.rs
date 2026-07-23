//! Ownership-aware outcomes from synchronized producer admission.

use std::sync::Arc;

use kafka_client_core::OperationId;

use crate::{ProducerDeliveryObserver, producer::ProducerHostInvariantError};

use super::{
    super::{
        ProducerRecord, ProducerRejectionReason, admission::AdmittedExplicit,
        admission::ProducerAdmissionFailure,
    },
    ProducerShardWakeError,
    cancellation::ProducerCancellationPort,
    shard::ProducerShardState,
};

/// Committed admission retaining terminal observation, identity, and execution fault.
///
/// Once this value exists, core accepted ownership. Neither a host invariant
/// nor a wake failure may be translated into ownership-returning rejection.
#[must_use = "committed admission owns a terminal observer and accepted-operation state"]
pub(crate) struct ProducerPortAccepted {
    observer: ProducerDeliveryObserver,
    operation_id: Option<OperationId>,
    fault: Result<(), ProducerPortAcceptedFault>,
}

impl ProducerPortAccepted {
    pub(super) fn with_cancellation(mut self, shared: &Arc<ProducerShardState>) -> Self {
        if let Some(operation_id) = self.operation_id {
            self.observer = self
                .observer
                .with_cancellation(ProducerCancellationPort::new(
                    Arc::downgrade(shared),
                    operation_id,
                ));
        }
        self
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ProducerDeliveryObserver,
        Option<OperationId>,
        Result<(), ProducerPortAcceptedFault>,
    ) {
        (self.observer, self.operation_id, self.fault)
    }

    pub(super) fn with_wake(mut self, wake: Result<(), ProducerShardWakeError>) -> Self {
        if self.fault.is_ok() {
            self.fault = wake.map_err(ProducerPortAcceptedFault::Wake);
        }
        self
    }
}

impl std::fmt::Debug for ProducerPortAccepted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerPortAccepted")
            .field("observer", &self.observer)
            .field("operation_id", &self.operation_id)
            .field("fault", &self.fault)
            .finish()
    }
}

/// Post-ownership execution fault that cannot revoke admission.
#[derive(Debug)]
pub(crate) enum ProducerPortAcceptedFault {
    HostInvariant(ProducerHostInvariantError),
    Wake(ProducerShardWakeError),
}

/// Immediate admission failure with ownership state encoded in its variant.
#[derive(Debug)]
pub(crate) enum ProducerPortAdmissionError {
    Rejected(ProducerPortRejected),
    Poisoned(ProducerPortPoison),
}

/// Healthy local rejection that preserves caller ownership.
#[derive(Debug)]
pub(crate) struct ProducerPortRejected {
    reason: ProducerPortRejectionReason,
    record: ProducerRecord,
}

impl ProducerPortRejected {
    pub(crate) const fn reason(&self) -> ProducerPortRejectionReason {
        self.reason
    }

    pub(crate) fn into_record(self) -> ProducerRecord {
        self.record
    }
}

/// Healthy immediate rejection before any ownership transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerPortRejectionReason {
    Contended,
    Host(ProducerRejectionReason),
}

/// Fatal shard state, distinct from ordinary bounded backpressure.
#[derive(Debug)]
pub(crate) enum ProducerPortPoison {
    BeforeAdmission {
        reason: ProducerPortPoisonReason,
        record: ProducerRecord,
    },
    BeforeOwnership {
        error: ProducerHostInvariantError,
        record: ProducerRecord,
    },
}

/// Source of a poisoned admission boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerPortPoisonReason {
    ShardLock,
    Host(ProducerHostInvariantError),
}

#[allow(
    clippy::result_large_err,
    reason = "ownership-preserving rejection returns the intact record"
)]
pub(super) fn classify_admission(
    result: Result<AdmittedExplicit, ProducerAdmissionFailure>,
) -> Result<ProducerPortAccepted, ProducerPortAdmissionError> {
    match result {
        Ok(admitted) => Ok(ProducerPortAccepted {
            operation_id: Some(admitted.operation_id()),
            observer: admitted.into_delivery_observer(),
            fault: Ok(()),
        }),
        Err(ProducerAdmissionFailure::Rejected(rejected)) => {
            let reason = rejected.reason();
            let record = rejected.into_record();
            if let ProducerRejectionReason::HostPoisoned(error) = reason {
                return Err(poisoned_before(
                    record,
                    ProducerPortPoisonReason::Host(error),
                ));
            }
            Err(ProducerPortAdmissionError::Rejected(ProducerPortRejected {
                reason: ProducerPortRejectionReason::Host(reason),
                record,
            }))
        }
        Err(ProducerAdmissionFailure::Invariant(poisoned)) => {
            let (error, record) = poisoned.into_parts();
            Err(ProducerPortAdmissionError::Poisoned(
                ProducerPortPoison::BeforeOwnership { error, record },
            ))
        }
        Err(ProducerAdmissionFailure::AcceptedInvariant(poisoned)) => {
            let (error, operation_id, observer) = poisoned.into_parts();
            Ok(ProducerPortAccepted {
                observer: ProducerDeliveryObserver::from_completion(observer),
                operation_id,
                fault: Err(ProducerPortAcceptedFault::HostInvariant(error)),
            })
        }
    }
}

pub(super) fn rejected(
    record: ProducerRecord,
    reason: ProducerPortRejectionReason,
) -> ProducerPortAdmissionError {
    ProducerPortAdmissionError::Rejected(ProducerPortRejected { reason, record })
}

pub(super) fn poisoned_before(
    record: ProducerRecord,
    reason: ProducerPortPoisonReason,
) -> ProducerPortAdmissionError {
    ProducerPortAdmissionError::Poisoned(ProducerPortPoison::BeforeAdmission { reason, record })
}
