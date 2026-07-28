//! Synchronized transfer into the independently bounded FIFO waiting owner.

use std::time::Instant;

use kafka_client_core::{Moment, ProducerWaitingAdmissionError};

use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerRecord, ProducerRejectionReason,
        ingress::{
            ProducerShardLockError,
            outcome::{
                ProducerPortAccepted, ProducerPortAdmissionError, ProducerPortPoisonReason,
                ProducerPortRejectionReason, classify_waiting_admission, poisoned_before, rejected,
            },
        },
    },
};

use super::{ProducerAdmissionPort, closed};

impl ProducerAdmissionPort {
    /// Waits out shard-lock contention, then transfers one ergonomic caller
    /// into the independently bounded FIFO waiting owner.
    #[allow(
        clippy::result_large_err,
        reason = "ownership-preserving rejection returns the intact record"
    )]
    pub(crate) fn admit_waiting(
        &self,
        attempted_at: Moment,
        deadline: OperationDeadline,
        record: ProducerRecord,
    ) -> Result<ProducerPortAccepted, ProducerPortAdmissionError> {
        if self.shared.admission_is_closed() {
            return Err(closed(record));
        }
        let mut data = match self.shared.data() {
            Ok(data) => data,
            Err(ProducerShardLockError::Contended) => {
                return Err(rejected(record, ProducerPortRejectionReason::Contended));
            }
            Err(ProducerShardLockError::Poisoned) => {
                return Err(poisoned_before(record, ProducerPortPoisonReason::ShardLock));
            }
        };
        if Instant::now() >= deadline.transport() {
            return Err(rejected(
                record,
                ProducerPortRejectionReason::Host(ProducerRejectionReason::Waiting(
                    ProducerWaitingAdmissionError::DeadlineElapsed,
                )),
            ));
        }
        let accepted = classify_waiting_admission(data.host.try_admit_waiting(
            attempted_at,
            deadline,
            record,
        ))?
        .with_cancellation(&self.shared);
        drop(data);
        Ok(accepted.with_wake(self.shared.wake()))
    }

    /// Transfers one caller into the independently bounded FIFO waiting owner.
    #[allow(
        clippy::result_large_err,
        reason = "ownership-preserving rejection returns the intact record"
    )]
    pub(crate) fn try_admit_waiting(
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
        let accepted = classify_waiting_admission(data.host.try_admit_waiting(
            attempted_at,
            deadline,
            record,
        ))?
        .with_cancellation(&self.shared);
        drop(data);
        Ok(accepted.with_wake(self.shared.wake()))
    }
}
