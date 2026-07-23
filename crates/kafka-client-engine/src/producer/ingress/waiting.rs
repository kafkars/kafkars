//! One-lock admission or bounded FIFO registration for an internal waiting send.

use kafka_client_core::{AdmissionRejection, Moment};

use crate::{
    clock::OperationDeadline,
    completion::CompletionRegistryError,
    producer::{
        ProducerRecord, ProducerRejectionReason, ProducerStoreError,
        pending::{PendingAdmissionRejected, PendingSendRegistration},
    },
};

use super::{
    ProducerAdmissionPort, ProducerPortAccepted, ProducerPortAdmissionError,
    ProducerPortPoisonReason, ProducerPortRejectionReason, ProducerShardLockError,
    outcome::{classify_admission, poisoned_before},
};

/// Exact ownership outcome from one waiting-send registration transaction.
#[must_use = "waiting admission retains a record, observer, or exact failure owner"]
pub(crate) enum ProducerWaitingStart {
    Accepted(ProducerPortAccepted),
    Pending(PendingSendRegistration),
    ImmediateFailure(ProducerPortAdmissionError),
    PendingRejected(PendingAdmissionRejected),
}

impl ProducerAdmissionPort {
    /// Attempts admission or registers one older-fenced pending send under one lock.
    pub(crate) fn start_waiting_explicit(
        &self,
        attempted_at: Moment,
        deadline: OperationDeadline,
        record: ProducerRecord,
    ) -> ProducerWaitingStart {
        let mut data = match self.data() {
            Ok(data) => data,
            Err(ProducerShardLockError::Poisoned | ProducerShardLockError::Contended) => {
                return ProducerWaitingStart::ImmediateFailure(poisoned_before(
                    record,
                    ProducerPortPoisonReason::ShardLock,
                ));
            }
        };
        let outcome = if data.has_pending() {
            register(&mut data, record, deadline)
        } else {
            match classify_admission(data.try_admit_explicit(attempted_at, deadline, record)) {
                Ok(accepted) => ProducerWaitingStart::Accepted(accepted),
                Err(ProducerPortAdmissionError::Rejected(rejected)) => {
                    if waits_for_capacity(rejected.reason()) {
                        register(&mut data, rejected.into_record(), deadline)
                    } else {
                        ProducerWaitingStart::ImmediateFailure(
                            ProducerPortAdmissionError::Rejected(rejected),
                        )
                    }
                }
                Err(poisoned) => ProducerWaitingStart::ImmediateFailure(poisoned),
            }
        };
        drop(data);
        match outcome {
            ProducerWaitingStart::Accepted(accepted) => {
                ProducerWaitingStart::Accepted(accepted.with_wake(self.wake()))
            }
            ProducerWaitingStart::Pending(registration) => {
                // Registration already owns the record, cell, deadline, and permit.
                // Wake failure cannot revoke them; the host's capped park supplies
                // bounded progress until that diagnostic gains a retained surface.
                let _wake_result = self.wake();
                ProducerWaitingStart::Pending(registration)
            }
            ready => ready,
        }
    }
}

fn register(
    data: &mut super::data::ProducerShardData,
    record: ProducerRecord,
    deadline: OperationDeadline,
) -> ProducerWaitingStart {
    match data.register_pending(record, deadline) {
        Ok(registration) => ProducerWaitingStart::Pending(registration),
        Err(rejected) => ProducerWaitingStart::PendingRejected(rejected),
    }
}

pub(super) const fn waits_for_capacity(reason: ProducerPortRejectionReason) -> bool {
    match reason {
        ProducerPortRejectionReason::Contended
        | ProducerPortRejectionReason::PendingPrecedence
        | ProducerPortRejectionReason::Host(ProducerRejectionReason::HostPoisoned(_)) => false,
        ProducerPortRejectionReason::Host(ProducerRejectionReason::Completion(error)) => {
            matches!(error, CompletionRegistryError::Full)
        }
        ProducerPortRejectionReason::Host(ProducerRejectionReason::Store(error)) => matches!(
            error,
            ProducerStoreError::RecordCapacity
                | ProducerStoreError::ByteCapacity
                | ProducerStoreError::BatchCapacity
        ),
        ProducerPortRejectionReason::Host(ProducerRejectionReason::Core(error)) => matches!(
            error,
            AdmissionRejection::ByteCapacity
                | AdmissionRejection::CompletionCapacity
                | AdmissionRejection::AccumulatorPending
        ),
    }
}
