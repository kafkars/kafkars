//! Immediate ownership-transfer admission into one synchronized producer shard.

mod waiting;

use std::sync::Arc;

use kafka_client_core::{AdmissionRejection, Moment};

use crate::clock::OperationDeadline;

use super::{
    super::{ProducerRecord, ProducerRejectionReason},
    ProducerShardLockError,
    flush_outcome::{ProducerPortFlushAccepted, ProducerPortFlushError, classify_flush},
    outcome::{
        ProducerPortAccepted, ProducerPortAdmissionError, ProducerPortBatchAdmission,
        ProducerPortBatchRejection, ProducerPortPoisonReason, ProducerPortRejectionReason,
        classify_admission, classify_waiting_admission, poisoned_before, rejected,
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
        self.shared.publish_admission_closed(&data);
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
        if self.shared.admission_is_closed() {
            return Err(closed(record));
        }
        let mut data = match self.shared.try_data() {
            Ok(data) => data,
            Err(ProducerShardLockError::Contended) => {
                return Err(rejected(record, ProducerPortRejectionReason::Contended));
            }
            Err(ProducerShardLockError::Poisoned) => {
                return Err(poisoned_before(record, ProducerPortPoisonReason::ShardLock));
            }
        };
        let accepted = classify_admission(data.try_admit_explicit(attempted_at, deadline, record))?
            .with_cancellation(&self.shared);
        drop(data);
        Ok(accepted.with_wake(self.shared.wake()))
    }

    /// Admits one ordered prefix under one shard lock and requests one host turn.
    ///
    /// The first record-level rejection stops admission. Its exact record and
    /// every untouched suffix record remain caller-owned in original order.
    pub(crate) fn try_admit_batch<I>(
        &self,
        attempted_at: Moment,
        deadline: OperationDeadline,
        records: I,
    ) -> ProducerPortBatchAdmission
    where
        I: IntoIterator<Item = ProducerRecord>,
    {
        let mut records = records.into_iter().peekable();
        if records.peek().is_none() {
            return ProducerPortBatchAdmission::new(Vec::new(), None);
        }
        if self.shared.admission_is_closed() {
            return reject_batch(records.collect(), closed);
        }
        let mut data = match self.shared.try_data() {
            Ok(data) => data,
            Err(ProducerShardLockError::Contended) => {
                return reject_batch(records.collect(), |record| {
                    rejected(record, ProducerPortRejectionReason::Contended)
                });
            }
            Err(ProducerShardLockError::Poisoned) => {
                return reject_batch(records.collect(), |record| {
                    poisoned_before(record, ProducerPortPoisonReason::ShardLock)
                });
            }
        };
        let mut accepted = Vec::new();
        let mut rejection = None;
        while let Some(record) = records.next() {
            let admission = if record.needs_partition() {
                classify_waiting_admission(data.host.try_admit_waiting(
                    attempted_at,
                    deadline,
                    record,
                ))
            } else {
                classify_admission(data.try_admit_explicit(attempted_at, deadline, record))
            };
            match admission {
                Ok(item) => accepted.push(item.with_cancellation(&self.shared)),
                Err(error) => {
                    rejection = Some(ProducerPortBatchRejection::new(error, records.collect()));
                    break;
                }
            }
        }
        drop(data);
        if let Some(first) = accepted.first_mut() {
            first.apply_wake(self.shared.wake());
        }
        ProducerPortBatchAdmission::new(accepted, rejection)
    }

    /// Attempts immediate flush admission with one shared completion reservation.
    pub(crate) fn try_admit_flush(
        &self,
        attempted_at: Moment,
    ) -> Result<ProducerPortFlushAccepted, ProducerPortFlushError> {
        if self.shared.admission_is_closed() {
            return Err(ProducerPortFlushError::Rejected(
                super::super::flush::FlushRejectionReason::Closed,
            ));
        }
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
        if self.shared.admission_is_closed() {
            return Err(ProducerPortFlushError::Rejected(
                super::super::flush::FlushRejectionReason::Closed,
            ));
        }
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
        self.shared.publish_admission_closed(&data);
        drop(data);
        Ok(accepted.with_wake(self.shared.wake()))
    }
}

fn closed(record: ProducerRecord) -> ProducerPortAdmissionError {
    rejected(
        record,
        ProducerPortRejectionReason::Host(ProducerRejectionReason::Core(
            AdmissionRejection::Closed,
        )),
    )
}

fn reject_batch(
    records: Vec<ProducerRecord>,
    classify: impl FnOnce(ProducerRecord) -> ProducerPortAdmissionError,
) -> ProducerPortBatchAdmission {
    let mut records = records.into_iter();
    let first = records
        .next()
        .unwrap_or_else(|| unreachable!("batch rejection requires one record"));
    ProducerPortBatchAdmission::new(
        Vec::new(),
        Some(ProducerPortBatchRejection::new(
            classify(first),
            records.collect(),
        )),
    )
}
