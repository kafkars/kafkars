//! Immediate ownership-transfer admission into one synchronized producer shard.

use std::sync::Arc;

use kafka_client_core::Moment;

use crate::clock::OperationDeadline;

use super::{
    super::ProducerRecord,
    ProducerShardLockError, ProducerShardWakeError,
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

    pub(super) fn data(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, super::data::ProducerShardData>, ProducerShardLockError>
    {
        self.shared.data()
    }

    pub(super) fn wake(&self) -> Result<(), ProducerShardWakeError> {
        self.shared.wake()
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

    #[cfg(test)]
    pub(crate) fn shard_lock_available_for_test(&self) -> bool {
        self.shared.try_data().is_ok()
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
        let mut data = match self.shared.try_data() {
            Ok(data) => data,
            Err(ProducerShardLockError::Contended) => {
                return Err(rejected(record, ProducerPortRejectionReason::Contended));
            }
            Err(ProducerShardLockError::Poisoned) => {
                return Err(poisoned_before(record, ProducerPortPoisonReason::ShardLock));
            }
        };
        if data.has_pending() {
            return Err(rejected(
                record,
                ProducerPortRejectionReason::PendingPrecedence,
            ));
        }
        let accepted = classify_admission(data.try_admit_explicit(attempted_at, deadline, record))?;
        drop(data);
        Ok(accepted.with_wake(self.shared.wake()))
    }
}
