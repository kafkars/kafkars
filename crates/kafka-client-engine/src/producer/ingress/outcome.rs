//! Ownership-aware outcomes from synchronized producer admission.

mod debug;
mod waiting;

use std::sync::Arc;

use kafka_client_core::OperationId;

use crate::{ProducerDeliveryObserver, producer::ProducerHostInvariantError};

use super::{
    super::{
        ProducerRecord, ProducerRejectionReason, admission::AdmittedExplicit,
        admission::ProducerAdmissionFailure, waiting::WaitingToken,
    },
    ProducerShardWakeError,
    cancellation::ProducerCancellationPort,
    shard::ProducerShardState,
};
pub(in crate::producer::ingress) use waiting::classify_waiting_admission;

/// Committed admission retaining terminal observation, identity, and execution fault.
///
/// Once this value exists, core accepted ownership. Neither a host invariant
/// nor a wake failure may be translated into ownership-returning rejection.
#[must_use = "committed admission owns a terminal observer and accepted-operation state"]
pub(crate) struct ProducerPortAccepted {
    observer: ProducerDeliveryObserver,
    operation_id: Option<OperationId>,
    waiting: Option<(kafka_client_core::ProducerWaiterId, Arc<WaitingToken>)>,
    fault: Result<(), ProducerPortAcceptedFault>,
}

/// One ordered prefix admitted under a single producer-shard lock.
pub(crate) struct ProducerPortBatchAdmission {
    accepted: Vec<ProducerPortAccepted>,
    rejection: Option<ProducerPortBatchRejection>,
}

impl ProducerPortBatchAdmission {
    pub(super) const fn new(
        accepted: Vec<ProducerPortAccepted>,
        rejection: Option<ProducerPortBatchRejection>,
    ) -> Self {
        Self {
            accepted,
            rejection,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<ProducerPortAccepted>,
        Option<ProducerPortBatchRejection>,
    ) {
        (self.accepted, self.rejection)
    }
}

/// First record-level rejection and every record not attempted after it.
pub(crate) struct ProducerPortBatchRejection {
    first: ProducerPortAdmissionError,
    remaining: Vec<ProducerRecord>,
}

impl ProducerPortBatchRejection {
    pub(super) const fn new(
        first: ProducerPortAdmissionError,
        remaining: Vec<ProducerRecord>,
    ) -> Self {
        Self { first, remaining }
    }

    pub(crate) fn into_parts(self) -> (ProducerPortAdmissionError, Vec<ProducerRecord>) {
        (self.first, self.remaining)
    }
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
        } else if let Some((waiter_id, token)) = &self.waiting {
            self.observer = self
                .observer
                .with_cancellation(ProducerCancellationPort::new_waiting(
                    Arc::downgrade(shared),
                    *waiter_id,
                    Arc::clone(token),
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
        self.apply_wake(wake);
        self
    }

    pub(super) fn apply_wake(&mut self, wake: Result<(), ProducerShardWakeError>) {
        if self.fault.is_ok() {
            self.fault = wake.map_err(ProducerPortAcceptedFault::Wake);
        }
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
            waiting: None,
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
                waiting: None,
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
