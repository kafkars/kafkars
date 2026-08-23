//! Public lossless rejection for share acknowledgement normalization.

use crate::bridge::share_consumer::{
    ShareAcknowledgementBuildError as BridgeBuildError,
    ShareAcknowledgementBuildErrorKind as BridgeBuildErrorKind,
};

use super::{ShareConsumerBatch, ShareRecordDecision};

/// Stable reason an exact share batch cannot become an acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcknowledgementBuildErrorKind {
    /// The batch retained no acquired ranges.
    EmptyAcquisitions,
    /// The caller supplied no application record decisions.
    EmptyDecisions,
    /// Acquisitions from different broker sessions were mixed.
    MixedSession,
    /// A decision used a negative Kafka offset.
    InvalidOffset,
    /// A decision named no acquisition in the batch.
    UnknownAcquisition,
    /// A decision offset fell outside its acquisition range.
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

impl From<BridgeBuildErrorKind> for ShareAcknowledgementBuildErrorKind {
    fn from(value: BridgeBuildErrorKind) -> Self {
        match value {
            BridgeBuildErrorKind::EmptyAcquisitions => Self::EmptyAcquisitions,
            BridgeBuildErrorKind::EmptyDecisions => Self::EmptyDecisions,
            BridgeBuildErrorKind::MixedSession => Self::MixedSession,
            BridgeBuildErrorKind::InvalidOffset => Self::InvalidOffset,
            BridgeBuildErrorKind::UnknownAcquisition => Self::UnknownAcquisition,
            BridgeBuildErrorKind::OffsetOutsideRange => Self::OffsetOutsideRange,
            BridgeBuildErrorKind::DuplicateDecision => Self::DuplicateDecision,
            BridgeBuildErrorKind::MissingDecision => Self::MissingDecision,
            BridgeBuildErrorKind::AllocationFailed => Self::AllocationFailed,
            BridgeBuildErrorKind::AccountingInvariant => Self::AccountingInvariant,
        }
    }
}

/// Normalization rejection retaining the exact batch and caller decisions.
#[must_use = "a rejected acknowledgement build still owns the exact share batch"]
pub struct ShareAcknowledgementBuildError {
    inner: BridgeBuildError,
}

impl ShareAcknowledgementBuildError {
    pub(crate) const fn from_bridge(inner: BridgeBuildError) -> Self {
        Self { inner }
    }

    /// Returns the stable normalization rejection category.
    pub fn kind(&self) -> ShareAcknowledgementBuildErrorKind {
        self.inner.kind().into()
    }

    /// Recovers the exact batch and caller decisions without reconstruction.
    pub fn into_parts(self) -> (ShareConsumerBatch, Vec<ShareRecordDecision>) {
        let (batch, decisions) = self.inner.into_parts();
        (
            ShareConsumerBatch::from_bridge(batch),
            decisions
                .into_iter()
                .map(ShareRecordDecision::from_bridge)
                .collect(),
        )
    }
}

impl std::fmt::Debug for ShareAcknowledgementBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgementBuildError")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ShareAcknowledgementBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "share acknowledgement build failed: {:?}",
            self.kind()
        )
    }
}

impl std::error::Error for ShareAcknowledgementBuildError {}
