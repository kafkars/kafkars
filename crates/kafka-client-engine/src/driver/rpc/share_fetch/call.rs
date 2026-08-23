//! Linear submission, completion, and shutdown recovery ownership for `ShareFetch`.

use kafka_client_core::{Moment, ShareFetchBrokerId};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::{ShareFetchRequest, ShareFetchResponse};

use crate::{
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::consumer::share_fetch::{PreparedShareFetchRequest, ShareFetchCorrelation},
};

use super::{
    submission::{ShareFetchDriverSubmitError, ShareFetchDriverSubmitErrorKind},
    terminal::ShareFetchRawTerminal,
};

/// Correlation facts retained on every accepted and rejected submission path.
#[must_use = "ShareFetch call evidence must be submitted, settled, or recovered"]
pub(crate) struct ShareFetchCallEvidence {
    pub(super) broker_id: ShareFetchBrokerId,
    pub(super) submitted_at: Moment,
    pub(super) correlation: ShareFetchCorrelation,
}

impl ShareFetchCallEvidence {
    pub(super) fn from_prepared(
        broker_id: ShareFetchBrokerId,
        submitted_at: Moment,
        prepared: PreparedShareFetchRequest,
    ) -> (ShareFetchRequest, Self) {
        let (request, correlation) = prepared.into_parts();
        (
            request,
            Self {
                broker_id,
                submitted_at,
                correlation,
            },
        )
    }
}

/// Definitely-unsent driver rejection retaining exact response correlation.
#[must_use = "a rejected ShareFetch submission retains settlement ownership"]
pub(crate) struct ShareFetchDriverSubmitFailure {
    evidence: ShareFetchCallEvidence,
    source: ShareFetchDriverSubmitError,
}

impl ShareFetchDriverSubmitFailure {
    pub(crate) const fn kind(&self) -> ShareFetchDriverSubmitErrorKind {
        self.source.kind()
    }

    pub(crate) fn into_evidence(self) -> ShareFetchCallEvidence {
        self.evidence
    }
}

/// Linear ownership of one accepted broker-local `ShareFetch` request.
#[must_use = "an accepted ShareFetch call must settle or recover"]
pub(crate) struct ShareFetchCall {
    evidence: Option<ShareFetchCallEvidence>,
    call: Option<RoutedCall<ShareFetchResponse>>,
}

impl ShareFetchCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        broker_id: ShareFetchBrokerId,
        prepared: PreparedShareFetchRequest,
        submitted_at: Moment,
        deadline: OperationDeadline,
    ) -> Result<Self, ShareFetchDriverSubmitFailure> {
        let (request, evidence) =
            ShareFetchCallEvidence::from_prepared(broker_id, submitted_at, prepared);
        match driver.submit_tracked_share_fetch(broker_id, request, deadline) {
            Ok(call) => Ok(Self {
                evidence: Some(evidence),
                call: Some(call),
            }),
            Err(source) => Err(ShareFetchDriverSubmitFailure { evidence, source }),
        }
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ShareFetchRawTerminal, ShareFetchCompletionFailure>> {
        let result = self.call.as_ref()?.try_result()?;
        drop(self.call.take());
        let evidence = self
            .evidence
            .take()
            .unwrap_or_else(|| unreachable!("live ShareFetch call retains evidence"));
        Some(match result {
            Ok(outcome) => Ok(ShareFetchRawTerminal::from_outcome(evidence, outcome)),
            Err(source) => Err(ShareFetchCompletionFailure { evidence, source }),
        })
    }

    pub(crate) fn recover_after_driver_shutdown(mut self) -> ShareFetchRecoveredCall {
        drop(self.call.take());
        ShareFetchRecoveredCall {
            evidence: self
                .evidence
                .take()
                .unwrap_or_else(|| unreachable!("unsettled ShareFetch call retains evidence")),
        }
    }
}

/// Completion-channel failure retaining accepted call evidence.
#[must_use = "ShareFetch completion failure retains settlement ownership"]
pub(crate) struct ShareFetchCompletionFailure {
    evidence: ShareFetchCallEvidence,
    source: CompletionError,
}

impl ShareFetchCompletionFailure {
    pub(crate) fn into_parts(self) -> (ShareFetchCallEvidence, ShareFetchCompletionErrorKind) {
        let Self { evidence, source } = self;
        (evidence, ShareFetchCompletionErrorKind::from_driver(source))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareFetchCompletionErrorKind {
    Closed,
    Consumed,
    Unknown,
}

impl ShareFetchCompletionErrorKind {
    const fn from_driver(source: CompletionError) -> Self {
        match source {
            CompletionError::Closed => Self::Closed,
            CompletionError::Consumed => Self::Consumed,
            _ => Self::Unknown,
        }
    }
}

/// Accepted ownership recovered only after the unique driver is gone.
#[must_use = "recovered ShareFetch ownership still requires settlement"]
pub(crate) struct ShareFetchRecoveredCall {
    evidence: ShareFetchCallEvidence,
}

impl ShareFetchRecoveredCall {
    pub(crate) fn into_evidence(self) -> ShareFetchCallEvidence {
        self.evidence
    }
}
