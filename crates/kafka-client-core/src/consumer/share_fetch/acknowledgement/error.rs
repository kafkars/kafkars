//! Lossless acknowledgement-build rejection with exact caller ownership.

use core::fmt;

use super::super::ShareAcquisition;
use super::ShareRecordDecision;

/// Stable reason an exact share batch cannot become an acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcknowledgementBuildErrorKind {
    /// No linear acquisitions were supplied.
    EmptyAcquisitions,
    /// No application record decisions were supplied.
    EmptyDecisions,
    /// Acquisitions from different broker sessions were mixed.
    MixedSession,
    /// A decision used a negative Kafka offset.
    InvalidOffset,
    /// A decision named no acquisition in this batch.
    UnknownAcquisition,
    /// A decision offset falls outside its acquisition range.
    OffsetOutsideRange,
    /// The same acquired record was decided more than once.
    DuplicateDecision,
    /// An acquisition had no application record decision.
    MissingDecision,
    /// Bounded normalization storage could not be reserved.
    AllocationFailed,
    /// Offset cardinality could not be represented locally.
    AccountingInvariant,
}

/// Build rejection retaining every acquisition and caller decision.
#[must_use = "a rejected acknowledgement still owns its exact acquisitions"]
#[derive(Debug, Eq, PartialEq)]
pub struct ShareAcknowledgementBuildError {
    kind: ShareAcknowledgementBuildErrorKind,
    acquisitions: Vec<ShareAcquisition>,
    decisions: Vec<ShareRecordDecision>,
}

impl ShareAcknowledgementBuildError {
    pub(super) const fn new(
        kind: ShareAcknowledgementBuildErrorKind,
        acquisitions: Vec<ShareAcquisition>,
        decisions: Vec<ShareRecordDecision>,
    ) -> Self {
        Self {
            kind,
            acquisitions,
            decisions,
        }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> ShareAcknowledgementBuildErrorKind {
        self.kind
    }

    /// Recovers exact caller ownership without reconstruction.
    pub fn into_parts(self) -> (Vec<ShareAcquisition>, Vec<ShareRecordDecision>) {
        (self.acquisitions, self.decisions)
    }
}

impl fmt::Display for ShareAcknowledgementBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "share acknowledgement rejected: {:?}", self.kind)
    }
}

impl std::error::Error for ShareAcknowledgementBuildError {}
