//! Producer admission results that preserve caller ownership on rejection.

use crate::{ByteCount, Deadline, OperationId};

/// Why immediate producer admission returned caller ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejection {
    /// Producer admission has closed permanently.
    Closed,
    /// The public operation deadline had already elapsed.
    DeadlineElapsed,
    /// Retaining the record would exceed the producer byte budget.
    ByteCapacity,
    /// No terminal-completion slot is available.
    CompletionCapacity,
    /// Retained-byte arithmetic could not represent the requested reservation.
    ByteCountOverflow,
    /// The producer exhausted its monotonic operation identity space.
    IdentityExhausted,
}

/// Immediate admission failure that preserves caller ownership.
#[derive(Debug)]
pub struct TryAdmitError<T> {
    pub(crate) reason: AdmissionRejection,
    pub(crate) value: T,
}

impl<T> TryAdmitError<T> {
    /// Returns the semantic rejection reason.
    pub const fn reason(&self) -> AdmissionRejection {
        self.reason
    }

    /// Returns the rejection reason and original value.
    pub fn into_parts(self) -> (AdmissionRejection, T) {
        (self.reason, self.value)
    }
}

/// Value whose bytes and terminal completion are now owned by the producer.
#[derive(Debug)]
pub struct Admitted<T> {
    pub(crate) id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) bytes: ByteCount,
    pub(crate) value: T,
}

impl<T> Admitted<T> {
    /// Returns the stable operation identity.
    pub const fn id(&self) -> OperationId {
        self.id
    }

    /// Returns the absolute deadline captured at the public boundary.
    pub const fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Returns bytes charged to the producer budget.
    pub const fn bytes(&self) -> ByteCount {
        self.bytes
    }

    /// Returns the operation identity and admitted value for accumulation.
    pub fn into_parts(self) -> (OperationId, T) {
        (self.id, self.value)
    }
}
