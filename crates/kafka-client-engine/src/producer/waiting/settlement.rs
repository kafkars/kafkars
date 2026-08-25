//! Exact not-sent settlement and retryable publication for queued callers.

use kafka_client_core::{
    ProducerCancellationOutcome, ProducerInput, ProducerWaiterId, ProducerWaitingTerminal,
};

use super::model::WaitingTokenState;
use crate::producer::{
    ProducerHost, ProducerHostInvariantError, cancellation::ProducerHostCancelError,
};

impl ProducerHost {
    pub(in crate::producer) fn try_cancel_waiter(
        &mut self,
        id: ProducerWaiterId,
        token: &std::sync::Arc<super::WaitingToken>,
    ) -> Result<ProducerCancellationOutcome, ProducerHostCancelError> {
        let state = token.lock().map_err(|_| {
            ProducerHostCancelError::Invariant(
                self.poison(ProducerHostInvariantError::WaitingToken),
            )
        })?;
        match *state {
            WaitingTokenState::Waiting => {
                drop(state);
                self.settle_waiter(id, ProducerWaitingTerminal::Cancelled)
                    .map_err(ProducerHostCancelError::Invariant)?;
                Ok(ProducerCancellationOutcome::CancelledNotSent)
            }
            WaitingTokenState::Accepted(operation_id) => {
                drop(state);
                self.try_cancel_operation(operation_id)
                    .map(|accepted| accepted.outcome())
            }
            WaitingTokenState::Settled => Ok(ProducerCancellationOutcome::AlreadyTerminal),
            WaitingTokenState::Promoting => Err(ProducerHostCancelError::Invariant(
                self.poison(ProducerHostInvariantError::WaitingOwnership),
            )),
        }
    }

    pub(in crate::producer) fn settle_waiter(
        &mut self,
        id: ProducerWaiterId,
        terminal: ProducerWaitingTerminal,
    ) -> Result<bool, ProducerHostInvariantError> {
        let Some(entry) = self.waiting.remove(id) else {
            return Ok(false);
        };
        if self.waiting_policy.remove(id).is_none() {
            return Err(self.poison(ProducerHostInvariantError::WaitingOwnership));
        }
        match entry.token.lock() {
            Ok(mut state) => *state = WaitingTokenState::Settled,
            Err(_) => return Err(self.poison(ProducerHostInvariantError::WaitingToken)),
        }
        self.store
            .release_waiting_topic(entry.topic_id)
            .map_err(|error| self.poison(ProducerHostInvariantError::Store(error)))?;
        drop(entry.record);
        let input = match terminal {
            ProducerWaitingTerminal::Cancelled => ProducerInput::CancelRequested {
                operation_id: entry.operation_id,
            },
            ProducerWaitingTerminal::DeadlineElapsed
            | ProducerWaitingTerminal::Closed
            | ProducerWaitingTerminal::TopicIdentityMismatch
            | ProducerWaitingTerminal::MetadataUnavailable { .. } => {
                ProducerInput::WaitingTerminal {
                    operation_id: entry.operation_id,
                    terminal,
                }
            }
        };
        let transition = self
            .core
            .apply(input)
            .map_err(|error| self.poison(ProducerHostInvariantError::Core(error)))?;
        if terminal == ProducerWaitingTerminal::Cancelled
            && transition.cancellation_outcome()
                != Some(ProducerCancellationOutcome::CancelledNotSent)
        {
            return Err(self.poison(ProducerHostInvariantError::WaitingOwnership));
        }
        self.bindings
            .mark_waiting_terminal(entry.operation_id)
            .map_err(|error| self.poison(ProducerHostInvariantError::Binding(error)))?;
        self.interpret_time_free_transition(transition)?;
        Ok(true)
    }
}
