//! Atomic lifecycle and sequence ownership for one transactional send.

use kafka_client_core::{
    Deadline, Moment, TransactionEpoch, TransactionLifecycleEffect, TransactionLifecycleInput,
    TransactionLifecycleMachineError, TransactionLifecycleState, TransactionPartition,
    TransactionSendAttempt, TransactionSendAttemptFailure, TransactionSendId,
    TransactionSendIdentity, TransactionSendOutcome, TransactionSequenceLease,
    TransactionSequenceSettlement,
};

use super::host::{TransactionLifecycleHost, TransactionLifecycleHostError};

/// Exact core authorization for replacing one retained transactional send attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::transaction) struct TransactionSendReplacement {
    pub(in crate::transaction) previous: TransactionSendAttempt,
    pub(in crate::transaction) replacement: TransactionSendAttempt,
    pub(in crate::transaction) identity: TransactionSendIdentity,
    pub(in crate::transaction) not_before: Deadline,
}

impl TransactionLifecycleHost {
    pub(crate) fn prepare_send_attempt(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        identity: TransactionSendIdentity,
    ) -> Result<TransactionSendAttempt, TransactionLifecycleHostError> {
        let owner_id = self.owner_id()?;
        let transition = self.machine.apply(
            owner_id,
            TransactionLifecycleInput::SendPrepared {
                epoch,
                send_id,
                identity,
            },
        )?;
        if transition.into_effect().is_some() {
            return Err(TransactionLifecycleHostError::UnexpectedEffect);
        }
        Ok(TransactionSendAttempt::initial())
    }

    pub(in crate::transaction) fn authorize_send_replacement(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        attempt: TransactionSendAttempt,
        now: Moment,
        failure: TransactionSendAttemptFailure,
    ) -> Result<Option<TransactionSendReplacement>, TransactionLifecycleHostError> {
        let owner_id = self.owner_id()?;
        let transition = self.machine.apply(
            owner_id,
            TransactionLifecycleInput::SendAttemptFailed {
                epoch,
                send_id,
                attempt,
                now,
                failure,
            },
        )?;
        match transition.into_effect() {
            None => Ok(None),
            Some(TransactionLifecycleEffect::ReplaceSendAttempt {
                owner_id: effect_owner,
                epoch: effect_epoch,
                send_id: effect_send,
                previous,
                replacement,
                identity,
                not_before,
            }) if effect_owner == owner_id
                && effect_epoch == epoch
                && effect_send == send_id
                && previous == attempt =>
            {
                Ok(Some(TransactionSendReplacement {
                    previous,
                    replacement,
                    identity,
                    not_before,
                }))
            }
            Some(_) => Err(TransactionLifecycleHostError::UnexpectedEffect),
        }
    }

    pub(crate) fn accept_unsequenced_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.accept_send(epoch, send_id)
    }

    pub(crate) fn sequence_accepted_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        record_count: usize,
    ) -> Result<TransactionSequenceLease, TransactionLifecycleHostError> {
        self.machine.preflight_send_settlement(epoch, send_id)?;
        if self.machine.state() != TransactionLifecycleState::Active {
            return Err(TransactionLifecycleMachineError::InvalidState {
                state: self.machine.state(),
            }
            .into());
        }
        self.sequencing
            .try_lease(epoch, partition, record_count)
            .map_err(Into::into)
    }

    pub(crate) fn settle_unsequenced_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        outcome: TransactionSendOutcome,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.machine.preflight_send_settlement(epoch, send_id)?;
        self.settle_send(epoch, send_id, outcome)
    }

    pub(crate) fn accept_send_with_sequence(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        record_count: usize,
    ) -> Result<TransactionSequenceLease, TransactionLifecycleHostError> {
        let lease = self.sequencing.try_lease(epoch, partition, record_count)?;
        if let Err(error) = self.accept_send(epoch, send_id) {
            if self
                .sequencing
                .release_not_sent(epoch, partition, lease)
                .is_err()
            {
                unreachable!("the newly acquired exact sequence lease remains owned");
            }
            return Err(error);
        }
        Ok(lease)
    }

    pub(crate) fn settle_unproduced_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
        outcome: TransactionSendOutcome,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.machine.preflight_send_settlement(epoch, send_id)?;
        match outcome {
            TransactionSendOutcome::Fatal => {
                self.sequencing
                    .preflight_accepted_settlement(epoch, partition, lease)?;
                self.sequencing.fence();
                let settled = self.sequencing.settle_accepted(
                    epoch,
                    partition,
                    lease,
                    TransactionSequenceSettlement::NotAppended,
                )?;
                if settled != TransactionSendOutcome::Fatal {
                    unreachable!("a fenced sequence owner settles every retained lease as fatal");
                }
            }
            TransactionSendOutcome::AbortRequired | TransactionSendOutcome::FailedHealthy => {
                self.sequencing
                    .preflight_not_sent_release(epoch, partition, lease)?;
                self.sequencing.release_not_sent(epoch, partition, lease)?;
            }
            TransactionSendOutcome::Succeeded => {
                return Err(TransactionLifecycleHostError::UnexpectedEffect);
            }
        }
        self.settle_send(epoch, send_id, outcome)
    }

    pub(crate) fn settle_accepted_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
        settlement: TransactionSequenceSettlement,
    ) -> Result<TransactionSendOutcome, TransactionLifecycleHostError> {
        self.machine.preflight_send_settlement(epoch, send_id)?;
        self.sequencing
            .preflight_accepted_settlement(epoch, partition, lease)?;
        let outcome = self
            .sequencing
            .settle_accepted(epoch, partition, lease, settlement)?;
        self.settle_send(epoch, send_id, outcome)?;
        Ok(outcome)
    }
}
