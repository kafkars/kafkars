//! Immediate ownership-transfer admission into one synchronized producer shard.

use std::sync::Arc;

use kafka_client_core::Moment;

use crate::clock::OperationDeadline;

use super::{
    super::ProducerRecord,
    ProducerShardLockError,
    flush_outcome::{ProducerPortFlushAccepted, ProducerPortFlushError, classify_flush},
    outcome::{
        ProducerPortAccepted, ProducerPortAdmissionError, ProducerPortPoisonReason,
        ProducerPortRejectionReason, classify_admission, poisoned_before, rejected,
    },
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
        let mut data = self.shared.data()?;
        data.close_admission();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn shard_stats(
        &self,
    ) -> Result<super::data::ProducerShardStats, ProducerShardLockError> {
        self.shared.data().map(|data| data.shard_stats())
    }

    #[cfg(test)]
    pub(crate) fn inject_terminal_interpretation_fault(
        &self,
    ) -> Result<(), ProducerShardLockError> {
        let mut data = self.shared.data()?;
        data.inject_terminal_interpretation_fault();
        Ok(())
    }

    /// Attempts immediate explicit-partition admission.
    ///
    /// `attempted_at` is captured once for this immediate attempt. `deadline`
    /// is the original public-boundary deadline and is never restarted. A
    /// Success means bytes, core identity, and terminal-completion capacity
    /// transferred atomically. Normal rejection returns the exact record.
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
        let mut data = match self.shared.try_data() {
            Ok(data) => data,
            Err(ProducerShardLockError::Contended) => {
                return Err(rejected(record, ProducerPortRejectionReason::Contended));
            }
            Err(ProducerShardLockError::Poisoned) => {
                return Err(poisoned_before(record, ProducerPortPoisonReason::ShardLock));
            }
        };
        let accepted = classify_admission(data.try_admit_explicit(attempted_at, deadline, record))?;
        drop(data);
        Ok(accepted.with_wake(self.shared.wake()))
    }

    /// Attempts immediate flush admission with one shared completion reservation.
    pub(crate) fn try_admit_flush(
        &self,
        attempted_at: Moment,
    ) -> Result<ProducerPortFlushAccepted, ProducerPortFlushError> {
        let mut data = match self.shared.try_data() {
            Ok(data) => data,
            Err(ProducerShardLockError::Contended) => {
                return Err(ProducerPortFlushError::Contended);
            }
            Err(ProducerShardLockError::Poisoned) => {
                return Err(ProducerPortFlushError::ShardPoisoned);
            }
        };
        let accepted = classify_flush(data.try_admit_flush(attempted_at))?;
        drop(data);
        Ok(accepted.with_wake(self.shared.wake()))
    }

    /// Attempts atomic producer close and drain-barrier admission.
    pub(crate) fn try_admit_close(
        &self,
        attempted_at: Moment,
    ) -> Result<ProducerPortFlushAccepted, ProducerPortFlushError> {
        let mut data = match self.shared.try_data() {
            Ok(data) => data,
            Err(ProducerShardLockError::Contended) => {
                return Err(ProducerPortFlushError::Contended);
            }
            Err(ProducerShardLockError::Poisoned) => {
                return Err(ProducerPortFlushError::ShardPoisoned);
            }
        };
        let accepted = classify_flush(data.try_admit_close(attempted_at))?;
        drop(data);
        Ok(accepted.with_wake(self.shared.wake()))
    }
}
