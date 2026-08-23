//! Linear acknowledgement admission, failure settlement, and lossless rejection.

use core::fmt;

use super::super::{
    ShareAcknowledgeAttempt, ShareAcquisitionAdmissionErrorKind, ShareAcquisitionRelease,
};
use super::ShareAcknowledgement;

/// Accepted core ownership for one exact `ShareAcknowledge` attempt.
#[must_use = "an admitted share acknowledgement must be submitted or settled"]
#[derive(Debug, Eq, PartialEq)]
pub struct ShareAcknowledgementAdmission {
    attempt: ShareAcknowledgeAttempt,
    acknowledgement: ShareAcknowledgement,
}

impl ShareAcknowledgementAdmission {
    pub(in crate::consumer::share_fetch) const fn new(
        attempt: ShareAcknowledgeAttempt,
        acknowledgement: ShareAcknowledgement,
    ) -> Self {
        Self {
            attempt,
            acknowledgement,
        }
    }

    /// Splits the exact attempt from its consumed application capability.
    pub fn into_parts(self) -> (ShareAcknowledgeAttempt, ShareAcknowledgement) {
        (self.attempt, self.acknowledgement)
    }
}

/// Stable rejection while admitting or settling one acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcknowledgementApplyErrorKind {
    /// The broker session is not ready for an acknowledgement.
    InvalidState,
    /// The original public deadline had elapsed before admission.
    DeadlineElapsed,
    /// The delivered records do not belong to the preceding live session epoch.
    SessionMismatch,
    /// The supplied attempt is not the one currently in flight.
    StaleAttempt,
    /// The next broker-session epoch cannot be represented.
    SessionEpochExhausted,
    /// The acquisition ledger rejected the exact capability set.
    Acquisition(ShareAcquisitionAdmissionErrorKind),
}

/// Lossless acknowledgement rejection retaining exact application ownership.
#[must_use = "a rejected acknowledgement still owns its exact capability"]
#[derive(Debug, Eq, PartialEq)]
pub struct ShareAcknowledgementApplyError {
    kind: ShareAcknowledgementApplyErrorKind,
    acknowledgement: ShareAcknowledgement,
}

impl ShareAcknowledgementApplyError {
    pub(in crate::consumer::share_fetch) const fn new(
        kind: ShareAcknowledgementApplyErrorKind,
        acknowledgement: ShareAcknowledgement,
    ) -> Self {
        Self {
            kind,
            acknowledgement,
        }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> ShareAcknowledgementApplyErrorKind {
        self.kind
    }

    /// Recovers the exact acknowledgement capability without reconstruction.
    pub fn into_acknowledgement(self) -> ShareAcknowledgement {
        self.acknowledgement
    }
}

impl fmt::Display for ShareAcknowledgementApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "share acknowledgement rejected: {:?}", self.kind)
    }
}

impl std::error::Error for ShareAcknowledgementApplyError {}

/// Delivery-authoritative settlement of one failed acknowledgement attempt.
#[must_use = "acknowledgement failure settlement retains retry or release ownership"]
#[derive(Debug, Eq, PartialEq)]
pub enum ShareAcknowledgementFailureSettlement {
    /// Transport definitely did not observe the request; the same capability may retry.
    Retry(ShareAcknowledgement),
    /// Transport may have observed the request; local ownership was retired conservatively.
    Lost(Vec<ShareAcquisitionRelease>),
}
