//! Private lossless translation for share acknowledgement capabilities.

use kafka_client_engine::share::{
    ShareAcknowledgement as EngineAcknowledgement,
    ShareAcknowledgementBuildErrorKind as EngineBuildErrorKind,
    ShareDisposition as EngineDisposition, ShareRecordDecision as EngineDecision,
};

use super::ShareConsumerBatch;

/// Private application disposition for one acquired record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareDisposition {
    Accept,
    Release,
    Reject,
}

impl ShareDisposition {
    const fn into_engine(self) -> EngineDisposition {
        match self {
            Self::Accept => EngineDisposition::Accept,
            Self::Release => EngineDisposition::Release,
            Self::Reject => EngineDisposition::Reject,
        }
    }

    const fn from_engine(value: EngineDisposition) -> Self {
        match value {
            EngineDisposition::Accept => Self::Accept,
            EngineDisposition::Release => Self::Release,
            EngineDisposition::Reject => Self::Reject,
        }
    }
}

/// Private record decision correlated to one exact acquisition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShareRecordDecision {
    inner: EngineDecision,
}

impl ShareRecordDecision {
    pub(super) const fn from_engine(inner: EngineDecision) -> Self {
        Self { inner }
    }

    const fn into_engine(self) -> EngineDecision {
        self.inner
    }

    pub(crate) const fn offset(self) -> i64 {
        self.inner.offset()
    }

    pub(crate) const fn disposition(self) -> ShareDisposition {
        ShareDisposition::from_engine(self.inner.disposition())
    }
}

/// Private linear acknowledgement capability ready for engine admission.
#[must_use = "dropping an acknowledgement sends nothing"]
pub(crate) struct ShareAcknowledgement {
    inner: EngineAcknowledgement,
}

impl ShareAcknowledgement {
    pub(super) const fn from_engine(inner: EngineAcknowledgement) -> Self {
        Self { inner }
    }

    pub(crate) fn acquisition_count(&self) -> usize {
        self.inner.acquisition_count()
    }

    pub(crate) fn range_count(&self) -> usize {
        self.inner.range_count()
    }

    pub(super) fn into_engine(self) -> EngineAcknowledgement {
        self.inner
    }
}

impl std::fmt::Debug for ShareAcknowledgement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgement")
            .field("acquisition_count", &self.acquisition_count())
            .field("range_count", &self.range_count())
            .finish_non_exhaustive()
    }
}

/// Private stable acknowledgement normalization category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareAcknowledgementBuildErrorKind {
    EmptyAcquisitions,
    EmptyDecisions,
    MixedSession,
    InvalidOffset,
    UnknownAcquisition,
    OffsetOutsideRange,
    DuplicateDecision,
    MissingDecision,
    AllocationFailed,
    AccountingInvariant,
}

impl From<EngineBuildErrorKind> for ShareAcknowledgementBuildErrorKind {
    fn from(value: EngineBuildErrorKind) -> Self {
        match value {
            EngineBuildErrorKind::EmptyAcquisitions => Self::EmptyAcquisitions,
            EngineBuildErrorKind::EmptyDecisions => Self::EmptyDecisions,
            EngineBuildErrorKind::MixedSession => Self::MixedSession,
            EngineBuildErrorKind::InvalidOffset => Self::InvalidOffset,
            EngineBuildErrorKind::UnknownAcquisition => Self::UnknownAcquisition,
            EngineBuildErrorKind::OffsetOutsideRange => Self::OffsetOutsideRange,
            EngineBuildErrorKind::DuplicateDecision => Self::DuplicateDecision,
            EngineBuildErrorKind::MissingDecision => Self::MissingDecision,
            EngineBuildErrorKind::AllocationFailed => Self::AllocationFailed,
            EngineBuildErrorKind::AccountingInvariant => Self::AccountingInvariant,
        }
    }
}

/// Private lossless normalization rejection.
#[must_use = "a rejected acknowledgement build still owns the exact share batch"]
pub(crate) struct ShareAcknowledgementBuildError {
    kind: ShareAcknowledgementBuildErrorKind,
    batch: Box<ShareConsumerBatch>,
    decisions: Vec<ShareRecordDecision>,
}

impl ShareAcknowledgementBuildError {
    pub(super) fn from_engine(
        error: kafka_client_engine::share::ShareAcknowledgementBuildError,
    ) -> Self {
        let kind = error.kind().into();
        let (batch, decisions) = error.into_parts();
        Self {
            kind,
            batch: Box::new(ShareConsumerBatch::from_engine(batch)),
            decisions: decisions
                .into_iter()
                .map(ShareRecordDecision::from_engine)
                .collect(),
        }
    }

    pub(crate) const fn kind(&self) -> ShareAcknowledgementBuildErrorKind {
        self.kind
    }

    pub(crate) fn into_parts(self) -> (ShareConsumerBatch, Vec<ShareRecordDecision>) {
        (*self.batch, self.decisions)
    }
}

impl std::fmt::Debug for ShareAcknowledgementBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgementBuildError")
            .field("kind", &self.kind)
            .field("decision_count", &self.decisions.len())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ShareAcknowledgementBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "share acknowledgement build failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ShareAcknowledgementBuildError {}

pub(super) fn engine_decisions(decisions: Vec<ShareRecordDecision>) -> Vec<EngineDecision> {
    decisions
        .into_iter()
        .map(ShareRecordDecision::into_engine)
        .collect()
}

pub(super) const fn engine_disposition(disposition: ShareDisposition) -> EngineDisposition {
    disposition.into_engine()
}
