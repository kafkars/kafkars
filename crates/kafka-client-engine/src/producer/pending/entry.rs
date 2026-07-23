//! Linear pending records and local pre-admission terminal failures.

use std::{sync::Arc, time::Instant};

use kafka_client_core::Deadline;

use crate::ProducerDeliveryStatus;

use super::{
    super::ProducerRecord, PendingAdmissionId, PendingCellError, PendingPromotion, PendingSendCell,
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

    pub(crate) fn into_parts(self) -> (PendingAdmissionId, ProducerRecord, Deadline, Instant) {
        (self.id, self.record, self.deadline, self.absolute_instant)
    }

    pub(crate) fn begin_promotion(&self) -> Result<PendingPromotion, PendingCellError> {
        self.cell.begin_promotion()
    }

    pub(super) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(super) const fn sequence(&self) -> u64 {
        self.sequence
    }
}

/// Why an unadmitted pending record settled locally.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingLocalFailureKind {
    DeadlineElapsed,
    Shutdown,
}

/// Linear local outcome for work that never crossed core admission.
#[derive(Debug)]
pub(crate) struct PendingLocalFailure {
    kind: PendingLocalFailureKind,
    delivery_status: ProducerDeliveryStatus,
    pending: PendingAdmission,
}

impl PendingLocalFailure {
    pub(super) const fn new(kind: PendingLocalFailureKind, pending: PendingAdmission) -> Self {
        Self {
            kind,
            delivery_status: ProducerDeliveryStatus::NotSent,
            pending,
        }
    }

    pub(crate) const fn kind(&self) -> PendingLocalFailureKind {
        self.kind
    }

    pub(crate) const fn delivery_status(&self) -> ProducerDeliveryStatus {
        self.delivery_status
    }

    pub(crate) fn into_pending(self) -> PendingAdmission {
        self.pending
    }
}

/// Host disposition after attempting to return unadmitted work to its queue.
#[derive(Debug)]
pub(crate) enum PendingRestoreOutcome {
    /// The exact slot generation and ordering facts are live again.
    Restored,
    /// Close won, so the unadmitted record settled locally as not sent.
    Shutdown(PendingLocalFailure),
}
