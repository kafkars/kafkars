//! Linear call, terminal outcome, and fail-closed acknowledgement ownership facts.

use kafka_client_core::{
    DeliveryStatus, ShareAcknowledgeAttempt, ShareAcknowledgement, ShareAcknowledgementApplyError,
    ShareAcknowledgementApplyErrorKind, ShareAcknowledgementBatch, ShareAcquisitionBatchError,
};

use crate::driver::{
    ShareAcknowledgeCall, ShareAcknowledgeCompletionErrorKind, ShareAcknowledgeDriverFailureKind,
    ShareAcknowledgeDriverSubmitErrorKind, ShareAcknowledgeResolution,
};

use super::super::fetch_acknowledgement::ShareAcknowledgementPreparationFailureKind;

#[must_use = "an accepted ShareAcknowledge call must settle or recover"]
pub(in crate::consumer::share) struct ActiveShareAcknowledgementCall {
    pub(super) attempt: ShareAcknowledgeAttempt,
    pub(super) acknowledgement: ShareAcknowledgement,
    pub(super) call: ShareAcknowledgeCall,
}

#[derive(Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareAcknowledgementExecutionOutcome {
    Responded(ShareAcknowledgeResolution),
    Failed {
        kind: ShareAcknowledgementExecutionFailureKind,
        delivery: DeliveryStatus,
        retry: Option<ShareAcknowledgement>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareAcknowledgementExecutionFailureKind {
    Submit(ShareAcknowledgeDriverSubmitErrorKind),
    Driver(ShareAcknowledgeDriverFailureKind),
    Completion(ShareAcknowledgeCompletionErrorKind),
    BrokerMismatch,
    Core(ShareAcknowledgementApplyErrorKind),
    Preparation(ShareAcknowledgementPreparationFailureKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareAcknowledgementSubmissionTurn {
    Submitted,
    Backpressured,
    Terminal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareAcknowledgementExecutionPoll {
    Pending,
    Terminal,
}

/// Exact capabilities retained after an internal settlement invariant rejects ownership.
#[must_use = "an acknowledgement ownership fault must remain hosted until diagnosis"]
pub(in crate::consumer::share) enum ShareAcknowledgementOwnershipFault {
    Apply(ShareAcknowledgementApplyError),
    Abandon {
        error: ShareAcquisitionBatchError,
        batches: Vec<ShareAcknowledgementBatch>,
    },
}

impl ActiveShareAcknowledgementCall {
    pub(in crate::consumer::share) const fn deadline(&self) -> kafka_client_core::Deadline {
        self.attempt.deadline()
    }
}
