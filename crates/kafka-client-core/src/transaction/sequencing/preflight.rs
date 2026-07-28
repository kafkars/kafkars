//! Nonmutating exact-owner checks shared by transactional sequence settlement.

use crate::{TransactionEpoch, TransactionSequenceLease};

use super::{
    TransactionPartition, TransactionSequenceMachine, TransactionSequenceMachineError,
    TransactionSequenceState, machine::OwnedLease,
};

impl TransactionSequenceMachine {
    /// Validates an exact definitely-unsent release without mutation.
    pub fn preflight_not_sent_release(
        &self,
        epoch: TransactionEpoch,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
    ) -> Result<(), TransactionSequenceMachineError> {
        self.preflight_not_sent_release_owned(epoch, partition, lease)
            .map(|_owned| ())
    }

    pub(super) fn preflight_not_sent_release_owned(
        &self,
        epoch: TransactionEpoch,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
    ) -> Result<OwnedLease, TransactionSequenceMachineError> {
        self.require_active(epoch)?;
        self.require_lease(epoch, partition, lease)
    }

    /// Validates an exact accepted terminal, including a late fatal drain.
    pub fn preflight_accepted_settlement(
        &self,
        epoch: TransactionEpoch,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
    ) -> Result<(), TransactionSequenceMachineError> {
        self.preflight_accepted_settlement_owned(epoch, partition, lease)
            .map(|_owned| ())
    }

    pub(super) fn preflight_accepted_settlement_owned(
        &self,
        epoch: TransactionEpoch,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
    ) -> Result<OwnedLease, TransactionSequenceMachineError> {
        let owned = self.require_lease(epoch, partition, lease)?;
        if !matches!(self.state(), TransactionSequenceState::Fenced) {
            self.require_active(epoch)?;
        }
        Ok(owned)
    }
}
