//! Linear pending records and local pre-admission terminal failures.

use std::{sync::Arc, time::Instant};

use kafka_client_core::Deadline;

use super::{
    super::ProducerRecord, PendingAdmissionId, PendingCellError, PendingNotificationJob,
    PendingSendCell, ProducerSendFailure, ProducerSendFailureKind, promotion::PendingPromotion,
};

/// One engine-owned record that has not crossed deterministic admission.
#[derive(Debug)]
pub(crate) struct PendingAdmission {
    id: PendingAdmissionId,
    record: ProducerRecord,
    deadline: Deadline,
    absolute_instant: Instant,
    retained_bytes: usize,
    sequence: u64,
    cell: Arc<PendingSendCell>,
}

/// Non-byte facts retained while the exact record attempts core admission.
#[derive(Debug)]
pub(super) struct PendingAdmissionFacts {
    id: PendingAdmissionId,
    deadline: Deadline,
    absolute_instant: Instant,
    retained_bytes: usize,
    sequence: u64,
    cell: Arc<PendingSendCell>,
}

impl PendingAdmission {
    pub(super) const fn new(
        id: PendingAdmissionId,
        record: ProducerRecord,
        deadline: Deadline,
        absolute_instant: Instant,
        retained_bytes: usize,
        sequence: u64,
        cell: Arc<PendingSendCell>,
    ) -> Self {
        Self {
            id,
            record,
            deadline,
            absolute_instant,
            retained_bytes,
            sequence,
            cell,
        }
    }

    pub(crate) const fn id(&self) -> PendingAdmissionId {
        self.id
    }

    pub(crate) const fn deadline(&self) -> Deadline {
        self.deadline
    }

    pub(crate) const fn absolute_instant(&self) -> Instant {
        self.absolute_instant
    }

    pub(crate) fn into_record(self) -> ProducerRecord {
        self.record
    }

    pub(super) fn begin_promotion(&self) -> Result<PendingPromotion, PendingCellError> {
        self.cell.begin_promotion()
    }

    pub(super) fn is_abandoned(&self) -> bool {
        self.cell.is_abandoned()
    }

    pub(super) fn into_transfer_parts(self) -> (PendingAdmissionFacts, ProducerRecord) {
        let facts = PendingAdmissionFacts {
            id: self.id,
            deadline: self.deadline,
            absolute_instant: self.absolute_instant,
            retained_bytes: self.retained_bytes,
            sequence: self.sequence,
            cell: self.cell,
        };
        (facts, self.record)
    }

    pub(super) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(super) const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[cfg(test)]
    pub(super) fn cell_for_test(&self) -> Arc<PendingSendCell> {
        Arc::clone(&self.cell)
    }

    #[cfg(test)]
    pub(super) fn topic_for_test(&self) -> &str {
        self.record.topic().as_ref()
    }
}

impl PendingAdmissionFacts {
    pub(super) fn restore(self, record: ProducerRecord) -> PendingAdmission {
        PendingAdmission {
            id: self.id,
            record,
            deadline: self.deadline,
            absolute_instant: self.absolute_instant,
            retained_bytes: self.retained_bytes,
            sequence: self.sequence,
            cell: self.cell,
        }
    }

    #[cfg(test)]
    pub(super) fn cell_for_test(&self) -> Arc<PendingSendCell> {
        Arc::clone(&self.cell)
    }
}

/// Linear local outcome for work that never crossed core admission.
pub(crate) struct PendingLocalFailure {
    failure: ProducerSendFailure,
    pending: PendingAdmission,
    notification: PendingNotificationJob,
}

impl PendingLocalFailure {
    pub(super) const fn new(
        failure: ProducerSendFailure,
        pending: PendingAdmission,
        notification: PendingNotificationJob,
    ) -> Self {
        Self {
            failure,
            pending,
            notification,
        }
    }

    pub(crate) const fn kind(&self) -> ProducerSendFailureKind {
        self.failure.kind()
    }

    pub(crate) const fn failure(&self) -> ProducerSendFailure {
        self.failure
    }

    pub(crate) fn into_parts(self) -> (PendingAdmission, PendingNotificationJob) {
        (self.pending, self.notification)
    }
}
