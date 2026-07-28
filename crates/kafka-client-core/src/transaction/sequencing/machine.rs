//! Sole deterministic owner of transactional partition sequence leases.

use std::collections::BTreeMap;

use crate::{TransactionEpoch, TransactionSendOutcome, TransactionSequenceLease};

use super::{
    TransactionPartition, TransactionSequenceMachineError, TransactionSequenceSettlement,
    TransactionSequenceState,
};

#[derive(Debug, Clone, Copy)]
pub(super) struct OwnedLease {
    epoch: TransactionEpoch,
    lease: TransactionSequenceLease,
    new_partition: bool,
}

/// Bounded producer-lifetime sequence owner with transaction-epoch admission.
#[derive(Debug)]
pub struct TransactionSequenceMachine {
    state: TransactionSequenceState,
    partition_capacity: usize,
    next_sequences: BTreeMap<TransactionPartition, i32>,
    leases: BTreeMap<TransactionPartition, OwnedLease>,
}

impl TransactionSequenceMachine {
    /// Creates one idle owner with a fixed distinct-partition envelope.
    pub fn try_new(partition_capacity: usize) -> Result<Self, TransactionSequenceMachineError> {
        if partition_capacity == 0 {
            return Err(TransactionSequenceMachineError::ZeroCapacity);
        }
        Ok(Self {
            state: TransactionSequenceState::Idle,
            partition_capacity,
            next_sequences: BTreeMap::new(),
            leases: BTreeMap::new(),
        })
    }

    /// Returns the current exact admission phase.
    pub const fn state(&self) -> TransactionSequenceState {
        self.state
    }

    /// Returns the number of exact outstanding sequence owners.
    pub fn outstanding_lease_count(&self) -> usize {
        self.leases.len()
    }

    /// Opens sequence admission for the exact lifecycle epoch.
    pub fn activate(
        &mut self,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionSequenceMachineError> {
        match self.state {
            TransactionSequenceState::Idle if self.leases.is_empty() => {
                self.state = TransactionSequenceState::Active(epoch);
                Ok(())
            }
            TransactionSequenceState::Idle => {
                Err(TransactionSequenceMachineError::OutstandingLeases)
            }
            TransactionSequenceState::Active(_) => {
                Err(TransactionSequenceMachineError::AlreadyActive)
            }
            TransactionSequenceState::Fenced => Err(TransactionSequenceMachineError::Fenced),
        }
    }

    /// Closes one settled epoch without resetting producer-lifetime sequences.
    pub fn release(
        &mut self,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionSequenceMachineError> {
        self.require_active(epoch)?;
        if !self.leases.is_empty() {
            return Err(TransactionSequenceMachineError::OutstandingLeases);
        }
        self.state = TransactionSequenceState::Idle;
        Ok(())
    }

    /// Acquires one nonempty exact sequence range for a topic-partition.
    pub fn try_lease(
        &mut self,
        epoch: TransactionEpoch,
        partition: TransactionPartition,
        record_count: usize,
    ) -> Result<TransactionSequenceLease, TransactionSequenceMachineError> {
        self.require_active(epoch)?;
        if self.leases.contains_key(&partition) {
            return Err(TransactionSequenceMachineError::PartitionBusy);
        }
        let count = u32::try_from(record_count)
            .ok()
            .filter(|count| *count != 0)
            .ok_or(TransactionSequenceMachineError::InvalidRecordCount)?;
        let new_partition = !self.next_sequences.contains_key(&partition);
        if new_partition && self.next_sequences.len() == self.partition_capacity {
            return Err(TransactionSequenceMachineError::PartitionCapacity);
        }
        let base_sequence = self.next_sequences.get(&partition).copied().unwrap_or(0);
        let lease = TransactionSequenceLease::try_new(base_sequence, count)
            .ok_or(TransactionSequenceMachineError::InvalidRecordCount)?;
        if new_partition {
            self.next_sequences.insert(partition, base_sequence);
        }
        self.leases.insert(
            partition,
            OwnedLease {
                epoch,
                lease,
                new_partition,
            },
        );
        Ok(lease)
    }

    /// Releases a definitely-unsent local attempt without changing transaction health.
    pub fn release_not_sent(
        &mut self,
        epoch: TransactionEpoch,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
    ) -> Result<(), TransactionSequenceMachineError> {
        let owned = self.preflight_not_sent_release_owned(epoch, partition, lease)?;
        self.leases.remove(&partition);
        if owned.new_partition {
            self.next_sequences.remove(&partition);
        }
        Ok(())
    }

    /// Settles one driver-accepted Produce and returns its lifecycle consequence.
    pub fn settle_accepted(
        &mut self,
        epoch: TransactionEpoch,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
        settlement: TransactionSequenceSettlement,
    ) -> Result<TransactionSendOutcome, TransactionSequenceMachineError> {
        let owned = self.preflight_accepted_settlement_owned(epoch, partition, lease)?;
        self.leases.remove(&partition);
        match settlement {
            TransactionSequenceSettlement::Succeeded => {
                if matches!(self.state, TransactionSequenceState::Fenced) {
                    return Ok(TransactionSendOutcome::Fatal);
                }
                self.next_sequences.insert(
                    partition,
                    advance_sequence(lease.base_sequence(), lease.record_count()),
                );
                Ok(TransactionSendOutcome::Succeeded)
            }
            TransactionSequenceSettlement::NotAppended => {
                if owned.new_partition {
                    self.next_sequences.remove(&partition);
                }
                if matches!(self.state, TransactionSequenceState::Fenced) {
                    Ok(TransactionSendOutcome::Fatal)
                } else {
                    Ok(TransactionSendOutcome::AbortRequired)
                }
            }
            TransactionSequenceSettlement::Uncertain => {
                self.state = TransactionSequenceState::Fenced;
                Ok(TransactionSendOutcome::Fatal)
            }
        }
    }

    /// Permanently closes admission while preserving exact leases for late drain.
    pub fn fence(&mut self) {
        self.state = TransactionSequenceState::Fenced;
    }

    pub(super) fn require_active(
        &self,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionSequenceMachineError> {
        match self.state {
            TransactionSequenceState::Active(active) if active == epoch => Ok(()),
            TransactionSequenceState::Active(_) => {
                Err(TransactionSequenceMachineError::EpochMismatch)
            }
            TransactionSequenceState::Idle => Err(TransactionSequenceMachineError::NotActive),
            TransactionSequenceState::Fenced => Err(TransactionSequenceMachineError::Fenced),
        }
    }

    pub(super) fn require_lease(
        &self,
        epoch: TransactionEpoch,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
    ) -> Result<OwnedLease, TransactionSequenceMachineError> {
        self.leases
            .get(&partition)
            .copied()
            .filter(|owned| owned.epoch == epoch && owned.lease == lease)
            .ok_or(TransactionSequenceMachineError::LeaseMismatch)
    }
}

fn advance_sequence(base: i32, count: u32) -> i32 {
    const SEQUENCE_DOMAIN: i64 = (i32::MAX as i64) + 1;
    let advanced = (i64::from(base) + i64::from(count)) % SEQUENCE_DOMAIN;
    i32::try_from(advanced).unwrap_or(0)
}
