//! Stable ordered outcome of one prefix-atomic producer batch admission.

use super::{
    ProducerTrySendAccepted, ProducerTrySendError, ProducerTrySendErrorKind, record::ProducerRecord,
};

/// One batch admission outcome in original record order.
///
/// `accepted` is always the exact admitted prefix. If present, `rejection`
/// owns the first rejected record and every untouched suffix record.
#[must_use = "accepted batch work owns terminal observers"]
pub struct ProducerTrySendBatch {
    accepted: Vec<ProducerTrySendAccepted>,
    rejection: Option<ProducerTrySendBatchError>,
}

impl ProducerTrySendBatch {
    pub(super) const fn new(
        accepted: Vec<ProducerTrySendAccepted>,
        rejection: Option<ProducerTrySendBatchError>,
    ) -> Self {
        Self {
            accepted,
            rejection,
        }
    }

    /// Transfers the accepted prefix and optional exact rejected suffix.
    pub fn into_parts(
        self,
    ) -> (
        Vec<ProducerTrySendAccepted>,
        Option<ProducerTrySendBatchError>,
    ) {
        (self.accepted, self.rejection)
    }
}

impl std::fmt::Debug for ProducerTrySendBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerTrySendBatch")
            .field("accepted", &self.accepted.len())
            .field("rejection", &self.rejection)
            .finish()
    }
}

/// First pre-admission failure plus its exact unaccepted record suffix.
#[derive(Debug)]
pub struct ProducerTrySendBatchError {
    kind: ProducerTrySendErrorKind,
    records: Vec<ProducerRecord>,
    detail: Option<String>,
}

impl ProducerTrySendBatchError {
    /// Returns the stable category of the first admission rejection.
    pub const fn kind(&self) -> ProducerTrySendErrorKind {
        self.kind
    }

    /// Borrows the exact first rejected record and untouched suffix.
    pub fn records(&self) -> &[ProducerRecord] {
        &self.records
    }

    /// Returns diagnostic detail for an internal mechanism fault.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    /// Recovers every unaccepted record in original order.
    pub fn into_records(self) -> Vec<ProducerRecord> {
        self.records
    }

    /// Transfers the stable reason, exact unaccepted suffix, and diagnostic.
    pub fn into_parts(
        self,
    ) -> (
        ProducerTrySendErrorKind,
        Vec<ProducerRecord>,
        Option<String>,
    ) {
        (self.kind, self.records, self.detail)
    }

    pub(super) fn from_single(
        error: ProducerTrySendError,
        mut remaining: Vec<ProducerRecord>,
    ) -> Self {
        let (kind, first, detail) = error.into_parts();
        remaining.insert(0, first);
        Self {
            kind,
            records: remaining,
            detail,
        }
    }

    pub(super) const fn from_parts(
        kind: ProducerTrySendErrorKind,
        records: Vec<ProducerRecord>,
        detail: Option<String>,
    ) -> Self {
        Self {
            kind,
            records,
            detail,
        }
    }
}
