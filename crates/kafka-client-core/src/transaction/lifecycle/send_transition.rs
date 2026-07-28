//! Accepted-send preparation, replacement, and terminal transitions.

use crate::{Moment, ProducerBrokerFailureKind};

use super::machine::OutstandingTransactionSend;
use super::{
    TransactionEpoch, TransactionLifecycleEffect, TransactionLifecycleMachine,
    TransactionLifecycleMachineError, TransactionLifecycleState, TransactionLifecycleTransition,
    TransactionSendAttempt, TransactionSendAttemptFailure, TransactionSendId,
    TransactionSendIdentity, TransactionSendOutcome,
};

impl TransactionLifecycleMachine {
    pub(super) fn accept_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        self.require_epoch(epoch)?;
        if self.state == TransactionLifecycleState::AbortRequired {
            return Err(TransactionLifecycleMachineError::AbortRequired);
        }
        self.require_state(TransactionLifecycleState::Active)?;
        if self.outstanding_sends.contains_key(&send_id) {
            return Err(TransactionLifecycleMachineError::DuplicateSend { send_id });
        }
        self.outstanding_sends
            .insert(send_id, OutstandingTransactionSend::accepted());
        Ok(TransactionLifecycleTransition::none())
    }

    pub(super) fn prepare_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        identity: TransactionSendIdentity,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        self.require_epoch(epoch)?;
        if self.state == TransactionLifecycleState::AbortRequired {
            return Err(TransactionLifecycleMachineError::AbortRequired);
        }
        self.require_state(TransactionLifecycleState::Active)?;
        let send = self
            .outstanding_sends
            .get_mut(&send_id)
            .ok_or(TransactionLifecycleMachineError::UnknownSend { send_id })?;
        if send.identity.is_some() {
            return Err(TransactionLifecycleMachineError::DuplicateSendPreparation { send_id });
        }
        send.identity = Some(identity);
        Ok(TransactionLifecycleTransition::none())
    }

    pub(super) fn fail_send_attempt(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        attempt: TransactionSendAttempt,
        now: Moment,
        failure: TransactionSendAttemptFailure,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        self.preflight_send_settlement(epoch, send_id)?;
        let send = self
            .outstanding_sends
            .get(&send_id)
            .copied()
            .ok_or(TransactionLifecycleMachineError::UnknownSend { send_id })?;
        if send.attempt != attempt {
            return Err(TransactionLifecycleMachineError::SendAttemptMismatch {
                expected: send.attempt,
                supplied: attempt,
            });
        }
        let identity = send
            .identity
            .ok_or(TransactionLifecycleMachineError::SendNotPrepared { send_id })?;
        if self.state != TransactionLifecycleState::Active
            || !matches!(
                failure,
                TransactionSendAttemptFailure::Broker(failure)
                    if failure.kind() == ProducerBrokerFailureKind::Routing
            )
            || send.replacements_started >= self.send_retry_policy.max_retries()
            || identity.deadline().is_elapsed_at(now)
        {
            return Ok(TransactionLifecycleTransition::none());
        }
        let Some(not_before) = now.checked_deadline_after(self.send_retry_policy.backoff_ticks())
        else {
            return Ok(TransactionLifecycleTransition::none());
        };
        if not_before >= identity.deadline() {
            return Ok(TransactionLifecycleTransition::none());
        }
        let replacement = attempt
            .next()
            .ok_or(TransactionLifecycleMachineError::SendAttemptExhausted)?;
        let replacements_started = send
            .replacements_started
            .checked_add(1)
            .ok_or(TransactionLifecycleMachineError::SendAttemptExhausted)?;
        let retained = self
            .outstanding_sends
            .get_mut(&send_id)
            .unwrap_or_else(|| unreachable!("preflight retained the exact send"));
        retained.attempt = replacement;
        retained.replacements_started = replacements_started;
        Ok(TransactionLifecycleTransition::one(
            TransactionLifecycleEffect::ReplaceSendAttempt {
                owner_id: self.owner_id,
                epoch,
                send_id,
                previous: attempt,
                replacement,
                identity,
                not_before,
            },
        ))
    }

    pub(super) fn settle_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        outcome: TransactionSendOutcome,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        self.preflight_send_settlement(epoch, send_id)?;
        let removed = self.outstanding_sends.remove(&send_id);
        debug_assert!(removed.is_some());
        if self.state == TransactionLifecycleState::Fatal {
            return Ok(TransactionLifecycleTransition::none());
        }
        if self.state == TransactionLifecycleState::DrainingFatal {
            if self.outstanding_sends.is_empty() {
                self.pending_end = None;
                self.active_epoch = None;
                self.state = TransactionLifecycleState::Closed;
                return Ok(TransactionLifecycleTransition::one(
                    TransactionLifecycleEffect::ReleaseOwner {
                        owner_id: self.owner_id,
                    },
                ));
            }
            return Ok(TransactionLifecycleTransition::none());
        }
        match outcome {
            TransactionSendOutcome::Fatal => Ok(self.enter_fatal()),
            TransactionSendOutcome::AbortRequired
                if self.state == TransactionLifecycleState::Active =>
            {
                self.state = TransactionLifecycleState::AbortRequired;
                Ok(TransactionLifecycleTransition::one(
                    TransactionLifecycleEffect::AbortRequired {
                        owner_id: self.owner_id,
                        epoch,
                    },
                ))
            }
            TransactionSendOutcome::Succeeded
            | TransactionSendOutcome::FailedHealthy
            | TransactionSendOutcome::AbortRequired => {
                if self.state == TransactionLifecycleState::DrainingAbort
                    && self.outstanding_sends.is_empty()
                {
                    Ok(self.submit_pending_end())
                } else {
                    Ok(TransactionLifecycleTransition::none())
                }
            }
        }
    }
}
