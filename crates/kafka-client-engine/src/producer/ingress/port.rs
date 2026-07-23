//! Immediate ownership-transfer admission into one synchronized producer shard.

use std::sync::Arc;

use kafka_client_core::Moment;

use crate::{ProducerDeliveryObserver, clock::OperationDeadline};

use super::{
    super::{
        ProducerHostInvariantError, ProducerRecord, ProducerRejectionReason,
        admission::ProducerAdmissionFailure,
    },
    ProducerShardLockError, ProducerShardWakeError,
    shard::ProducerShardState,
};

/// Cloneable, thread-safe producer admission capability for one shard.
#[derive(Clone)]
pub(crate) struct ProducerAdmissionPort {
    shared: Arc<ProducerShardState>,
}

impl ProducerAdmissionPort {
    pub(super) const fn new(shared: Arc<ProducerShardState>) -> Self {
        Self { shared }
    }

    /// Closes core admission before terminal host draining begins.
    pub(crate) fn close_admission(&self) -> Result<(), ProducerShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn host_stats(
        &self,
    ) -> Result<crate::producer::host::ProducerHostStats, ProducerShardLockError> {
        self.shared.host().map(|host| host.stats())
    }

    #[cfg(test)]
    pub(crate) fn inject_terminal_interpretation_fault(
        &self,
    ) -> Result<(), ProducerShardLockError> {
        let mut host = self.shared.host()?;
        host.inject_terminal_interpretation_fault();
        Ok(())
    }

    /// Attempts immediate explicit-partition admission.
    ///
    /// `attempted_at` is captured once for this immediate attempt. `deadline`
    /// is the original public-boundary deadline and is never restarted. A
    /// queued send must use its current promotion moment with that same
    /// deadline. Success means bytes, core identity, and terminal-completion
    /// capacity transferred atomically. Normal rejection returns the exact
    /// record.
    #[allow(
        clippy::result_large_err,
        reason = "ownership-preserving rejection returns the intact record"
    )]
    pub(crate) fn try_admit_explicit(
        &self,
        attempted_at: Moment,
        deadline: OperationDeadline,
        record: ProducerRecord,
    ) -> Result<ProducerPortAccepted, ProducerPortAdmissionError> {
        let mut host = match self.shared.try_host() {
            Ok(host) => host,
            Err(ProducerShardLockError::Contended) => {
                return Err(rejected(record, ProducerPortRejectionReason::Contended));
            }
            Err(ProducerShardLockError::Poisoned) => {
                return Err(poisoned_before(record, ProducerPortPoisonReason::ShardLock));
            }
        };
        let admitted = match host.try_admit_explicit(attempted_at, deadline, record) {
            Ok(admitted) => admitted,
            Err(ProducerAdmissionFailure::Rejected(rejected)) => {
                let reason = rejected.reason();
                let record = rejected.into_record();
                if let ProducerRejectionReason::HostPoisoned(error) = reason {
                    return Err(poisoned_before(
                        record,
                        ProducerPortPoisonReason::Host(error),
                    ));
                }
                return Err(super::ProducerPortAdmissionError::Rejected(
                    ProducerPortRejected {
                        reason: ProducerPortRejectionReason::Host(reason),
                        record,
                    },
                ));
            }
            Err(ProducerAdmissionFailure::Invariant(poisoned)) => {
                let (error, record) = poisoned.into_parts();
                return Err(super::ProducerPortAdmissionError::Poisoned(
                    ProducerPortPoison::BeforeOwnership { error, record },
                ));
            }
            Err(ProducerAdmissionFailure::AcceptedInvariant(poisoned)) => {
                let (error, operation_id, observer) = poisoned.into_parts();
                return Ok(ProducerPortAccepted {
                    observer: ProducerDeliveryObserver::from_completion(observer),
                    operation_id,
                    fault: Err(ProducerPortAcceptedFault::HostInvariant(error)),
                });
            }
        };
        let operation_id = admitted.operation_id();
        let observer = admitted.into_delivery_observer();
        drop(host);
        let fault = self.shared.wake().map_err(ProducerPortAcceptedFault::Wake);
        Ok(ProducerPortAccepted {
            observer,
            operation_id: Some(operation_id),
            fault,
        })
    }
}

/// Committed admission retaining terminal observation, identity, and execution fault.
///
/// Once this value exists, core accepted ownership. Neither a host invariant
/// nor a wake failure may be translated into ownership-returning rejection.
#[must_use = "committed admission owns a terminal observer and accepted-operation state"]
pub(crate) struct ProducerPortAccepted {
    observer: ProducerDeliveryObserver,
    operation_id: Option<kafka_client_core::OperationId>,
    fault: Result<(), ProducerPortAcceptedFault>,
}

impl ProducerPortAccepted {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ProducerDeliveryObserver,
        Option<kafka_client_core::OperationId>,
        Result<(), ProducerPortAcceptedFault>,
    ) {
        (self.observer, self.operation_id, self.fault)
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

fn rejected(
    record: ProducerRecord,
    reason: ProducerPortRejectionReason,
) -> ProducerPortAdmissionError {
    ProducerPortAdmissionError::Rejected(ProducerPortRejected { reason, record })
}

fn poisoned_before(
    record: ProducerRecord,
    reason: ProducerPortPoisonReason,
) -> ProducerPortAdmissionError {
    ProducerPortAdmissionError::Poisoned(ProducerPortPoison::BeforeAdmission { reason, record })
}
