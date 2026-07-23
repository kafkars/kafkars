//! Ownership-preserving rejection and invariant failures for pending admission.

use super::super::ProducerRecord;

/// Healthy reason a record could not enter bounded pending ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingAdmissionRejectionReason {
    Closed,
    CountCapacity,
    ByteCapacity,
    NotificationBackpressure,
    RetainedSizeOverflow,
    IdentityExhausted,
}

/// Rejection before ownership transfer, retaining the exact producer record.
#[derive(Debug)]
pub(crate) struct PendingAdmissionRejected {
    reason: PendingAdmissionRejectionReason,
    record: ProducerRecord,
}

impl PendingAdmissionRejected {
    pub(super) const fn new(
        reason: PendingAdmissionRejectionReason,
        record: ProducerRecord,
    ) -> Self {
        Self { reason, record }
    }

    pub(crate) const fn reason(&self) -> PendingAdmissionRejectionReason {
        self.reason
    }

    pub(crate) fn into_record(self) -> ProducerRecord {
        self.record
    }
}

/// Impossible disagreement inside the generation-fenced pending registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingRegistryError {
    UnknownSlot,
    StaleGeneration,
    SlotOccupied,
    VacancyIndex,
    CountCapacity,
    ByteCapacity,
    RetainedSizeOverflow,
    FifoPrecedence,
    IndexCollision,
    CorruptIndex,
    RetainedAccounting,
    ObservationState,
    Closed,
    StillOpen,
}
