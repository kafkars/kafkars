//! Linear submission, completion, and recovery ownership for `ShareAcknowledge`.

use kafka_client_core::ShareFetchBrokerId;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::{ShareAcknowledgeRequest, ShareAcknowledgeResponse};

use crate::{
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::consumer::share_acknowledge::{
        PreparedShareAcknowledgeRequest, ShareAcknowledgeCorrelation,
    },
};

use super::{
    submission::{ShareAcknowledgeDriverSubmitError, ShareAcknowledgeDriverSubmitErrorKind},
    terminal::ShareAcknowledgeRawTerminal,
};

/// Correlation retained on every accepted and rejected submission path.
#[must_use = "ShareAcknowledge evidence must be submitted, settled, or recovered"]
pub(super) struct ShareAcknowledgeCallEvidence {
    pub(super) broker_id: ShareFetchBrokerId,
    pub(super) correlation: ShareAcknowledgeCorrelation,
}

impl ShareAcknowledgeCallEvidence {
    pub(super) fn from_prepared(
        broker_id: ShareFetchBrokerId,
        prepared: PreparedShareAcknowledgeRequest,
    ) -> (ShareAcknowledgeRequest, Self) {
        let (request, correlation) = prepared.into_parts();
        (
            request,
            Self {
                broker_id,
                correlation,
            },
        )
    }
}

/// Definitely-unsent driver rejection retaining exact response correlation.
#[must_use = "a rejected ShareAcknowledge submission retains settlement ownership"]
pub(crate) struct ShareAcknowledgeDriverSubmitFailure {
    evidence: ShareAcknowledgeCallEvidence,
    source: ShareAcknowledgeDriverSubmitError,
}

impl ShareAcknowledgeDriverSubmitFailure {
    pub(crate) const fn kind(&self) -> ShareAcknowledgeDriverSubmitErrorKind {
        self.source.kind()
    }

    pub(crate) fn discard(self) {
        drop(self.evidence);
    }
}

/// Linear ownership of one accepted broker-local acknowledgement request.
#[must_use = "an accepted ShareAcknowledge call must settle or recover"]
pub(crate) struct ShareAcknowledgeCall {
    evidence: Option<ShareAcknowledgeCallEvidence>,
    call: Option<RoutedCall<ShareAcknowledgeResponse>>,
}

impl ShareAcknowledgeCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        broker_id: ShareFetchBrokerId,
        prepared: PreparedShareAcknowledgeRequest,
        deadline: OperationDeadline,
    ) -> Result<Self, ShareAcknowledgeDriverSubmitFailure> {
        let (request, evidence) = ShareAcknowledgeCallEvidence::from_prepared(broker_id, prepared);
        match driver.submit_tracked_share_acknowledge(broker_id, request, deadline) {
            Ok(call) => Ok(Self {
                evidence: Some(evidence),
                call: Some(call),
            }),
            Err(source) => Err(ShareAcknowledgeDriverSubmitFailure { evidence, source }),
        }
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ShareAcknowledgeRawTerminal, ShareAcknowledgeCompletionFailure>> {
        let result = self.call.as_ref()?.try_result()?;
        drop(self.call.take());
        let evidence = self
            .evidence
            .take()
            .unwrap_or_else(|| unreachable!("live ShareAcknowledge call retains evidence"));
        Some(match result {
            Ok(outcome) => Ok(ShareAcknowledgeRawTerminal::from_outcome(evidence, outcome)),
            Err(source) => Err(ShareAcknowledgeCompletionFailure { evidence, source }),
        })
    }

    pub(crate) fn recover_after_driver_shutdown(mut self) -> ShareAcknowledgeRecoveredCall {
        drop(self.call.take());
        ShareAcknowledgeRecoveredCall {
            evidence: self.evidence.take().unwrap_or_else(|| {
                unreachable!("unsettled ShareAcknowledge call retains evidence")
            }),
        }
    }
}

/// Completion-channel failure retaining accepted call evidence.
#[must_use = "ShareAcknowledge completion failure retains settlement ownership"]
pub(crate) struct ShareAcknowledgeCompletionFailure {
    evidence: ShareAcknowledgeCallEvidence,
    source: CompletionError,
}

impl ShareAcknowledgeCompletionFailure {
    pub(crate) fn into_kind(self) -> ShareAcknowledgeCompletionErrorKind {
        let Self { evidence, source } = self;
        drop(evidence);
        ShareAcknowledgeCompletionErrorKind::from_driver(source)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareAcknowledgeCompletionErrorKind {
    Closed,
    Consumed,
    Unknown,
}

impl ShareAcknowledgeCompletionErrorKind {
    const fn from_driver(source: CompletionError) -> Self {
        match source {
            CompletionError::Closed => Self::Closed,
            CompletionError::Consumed => Self::Consumed,
            _ => Self::Unknown,
        }
    }
}

/// Accepted ownership recovered only after the unique driver is gone.
#[must_use = "recovered ShareAcknowledge ownership still requires settlement"]
pub(crate) struct ShareAcknowledgeRecoveredCall {
    evidence: ShareAcknowledgeCallEvidence,
}

impl ShareAcknowledgeRecoveredCall {
    pub(crate) fn discard(self) {
        drop(self.evidence);
    }
}
