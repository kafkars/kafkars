//! Immediate non-owning access to stage-aware producer cancellation.

use std::sync::{Arc, Weak};

use kafka_client_core::{
    OperationId, ProducerCancellationOutcome as CoreCancellationOutcome, ProducerWaiterId,
};

use super::{ProducerShardLockError, ProducerShardWakeError, shard::ProducerShardState};
use crate::producer::cancellation::{ProducerHostCancelAccepted, ProducerHostCancelError};

/// Weak operation capability retained by the sole delivery observer.
pub(crate) struct ProducerCancellationPort {
    shared: Weak<ProducerShardState>,
    target: ProducerCancellationTarget,
    abandonment_armed: bool,
}

enum ProducerCancellationTarget {
    Active(OperationId),
    Waiting {
        waiter_id: ProducerWaiterId,
        token: Arc<crate::producer::waiting::WaitingToken>,
    },
}

impl ProducerCancellationPort {
    pub(super) const fn new(shared: Weak<ProducerShardState>, operation_id: OperationId) -> Self {
        Self {
            shared,
            target: ProducerCancellationTarget::Active(operation_id),
            abandonment_armed: false,
        }
    }

    pub(super) fn new_waiting(
        shared: Weak<ProducerShardState>,
        waiter_id: ProducerWaiterId,
        token: Arc<crate::producer::waiting::WaitingToken>,
    ) -> Self {
        Self {
            shared,
            target: ProducerCancellationTarget::Waiting { waiter_id, token },
            abandonment_armed: true,
        }
    }

    pub(crate) const fn complete_observation(&mut self) {
        self.abandonment_armed = false;
    }

    pub(crate) const fn disarm_abandonment(&mut self) {
        self.abandonment_armed = false;
    }

    pub(crate) fn try_cancel(&self) -> Result<ProducerPortCancelAccepted, ProducerPortCancelError> {
        let shared = self
            .shared
            .upgrade()
            .ok_or(ProducerPortCancelError::HostUnavailable)?;
        let mut data = match shared.try_data() {
            Ok(data) => data,
            Err(ProducerShardLockError::Contended) => {
                return Err(ProducerPortCancelError::Contended);
            }
            Err(ProducerShardLockError::Poisoned) => {
                return Err(ProducerPortCancelError::HostUnavailable);
            }
        };
        let accepted = match &self.target {
            ProducerCancellationTarget::Active(operation_id) => {
                classify(data.try_cancel(*operation_id))?
            }
            ProducerCancellationTarget::Waiting { waiter_id, token } => {
                classify_waiting(data.try_cancel_waiter(*waiter_id, token))?
            }
        };
        drop(data);
        if accepted.needs_wake() {
            Ok(accepted.with_wake(shared.wake()))
        } else {
            Ok(accepted)
        }
    }
}

impl std::fmt::Debug for ProducerCancellationPort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerCancellationPort")
            .field(
                "waiting",
                &matches!(self.target, ProducerCancellationTarget::Waiting { .. }),
            )
            .finish_non_exhaustive()
    }
}

impl Drop for ProducerCancellationPort {
    fn drop(&mut self) {
        if !self.abandonment_armed {
            return;
        }
        let ProducerCancellationTarget::Waiting { token, .. } = &self.target else {
            return;
        };
        if token.request_abandonment() {
            if let Some(shared) = self.shared.upgrade() {
                let _wake_result = shared.wake();
            }
        }
    }
}

/// Successful core cancellation decision and any post-decision wake fault.
pub(crate) struct ProducerPortCancelAccepted {
    outcome: CoreCancellationOutcome,
    fault: Option<ProducerPortCancelFault>,
}

impl ProducerPortCancelAccepted {
    pub(crate) const fn outcome(&self) -> CoreCancellationOutcome {
        self.outcome
    }

    pub(crate) fn into_fault(self) -> Option<ProducerPortCancelFault> {
        self.fault
    }

    const fn needs_wake(&self) -> bool {
        matches!(self.outcome, CoreCancellationOutcome::CancelledNotSent)
    }

    fn with_wake(mut self, wake: Result<(), ProducerShardWakeError>) -> Self {
        self.fault = wake.err().map(ProducerPortCancelFault::Wake);
        self
    }
}

impl std::fmt::Debug for ProducerPortCancelAccepted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerPortCancelAccepted")
            .field("outcome", &self.outcome)
            .field("fault", &self.fault)
            .finish()
    }
}

/// Post-decision cancellation fault that cannot revoke the outcome.
#[derive(Debug)]
pub(crate) enum ProducerPortCancelFault {
    Wake(ProducerShardWakeError),
}

/// Immediate failure before a cancellation outcome becomes observable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerPortCancelError {
    Contended,
    HostUnavailable,
    ExecutionGenerationExhausted,
    InternalInvariant(crate::producer::ProducerHostInvariantError),
}

fn classify(
    result: Result<ProducerHostCancelAccepted, ProducerHostCancelError>,
) -> Result<ProducerPortCancelAccepted, ProducerPortCancelError> {
    match result {
        Ok(accepted) => Ok(ProducerPortCancelAccepted {
            outcome: accepted.outcome(),
            fault: None,
        }),
        Err(ProducerHostCancelError::HostUnavailable(_)) => {
            Err(ProducerPortCancelError::HostUnavailable)
        }
        Err(ProducerHostCancelError::ExecutionGenerationExhausted) => {
            Err(ProducerPortCancelError::ExecutionGenerationExhausted)
        }
        Err(ProducerHostCancelError::Invariant(error)) => {
            Err(ProducerPortCancelError::InternalInvariant(error))
        }
    }
}

fn classify_waiting(
    result: Result<CoreCancellationOutcome, ProducerHostCancelError>,
) -> Result<ProducerPortCancelAccepted, ProducerPortCancelError> {
    match result {
        Ok(outcome) => Ok(ProducerPortCancelAccepted {
            outcome,
            fault: None,
        }),
        Err(ProducerHostCancelError::HostUnavailable(_)) => {
            Err(ProducerPortCancelError::HostUnavailable)
        }
        Err(ProducerHostCancelError::ExecutionGenerationExhausted) => {
            Err(ProducerPortCancelError::ExecutionGenerationExhausted)
        }
        Err(ProducerHostCancelError::Invariant(error)) => {
            Err(ProducerPortCancelError::InternalInvariant(error))
        }
    }
}
