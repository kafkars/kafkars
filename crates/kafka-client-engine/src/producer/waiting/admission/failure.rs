//! Ownership-preserving failures before bounded waiting admission commits.

use crate::producer::{ProducerHostInvariantError, ProducerRecord, ProducerRejectionReason};

/// Waiting rejection preserving exact record ownership.
pub(crate) struct RejectedWaiting {
    pub(crate) reason: ProducerRejectionReason,
    pub(crate) record: ProducerRecord,
}

/// Distinguishes normal bounded rejection from cleanup corruption.
pub(crate) enum ProducerWaitingAdmissionFailure {
    Rejected(RejectedWaiting),
    Invariant {
        error: ProducerHostInvariantError,
        record: ProducerRecord,
    },
}

pub(super) fn rejected(
    record: ProducerRecord,
    reason: ProducerRejectionReason,
) -> ProducerWaitingAdmissionFailure {
    ProducerWaitingAdmissionFailure::Rejected(RejectedWaiting { reason, record })
}
