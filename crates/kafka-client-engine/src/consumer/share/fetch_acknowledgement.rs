//! Core admission paired with exact generated `ShareAcknowledge` request ownership.

use kafka_client_core::{
    DeliveryStatus, ShareAcknowledgeAttempt, ShareAcknowledgement,
    ShareAcknowledgementApplyErrorKind, ShareAcknowledgementFailureSettlement,
};

use crate::{
    clock::DeadlineCapture,
    driver::share_acknowledge::{ShareAcknowledgeResolution, ShareAcknowledgeRoute},
    protocol::consumer::share_acknowledge::{
        PreparedShareAcknowledgeRequest, ShareAcknowledgeRequestFailure, share_acknowledge_request,
    },
};

use super::fetch_session::ShareFetchSessionOwner;

/// Core attempt, exact capability, generated request, and unchanged public deadline.
#[must_use = "a prepared share acknowledgement must be submitted or settled"]
pub(super) struct PreparedShareAcknowledgement {
    pub(super) attempt: ShareAcknowledgeAttempt,
    pub(super) acknowledgement: ShareAcknowledgement,
    pub(super) request: PreparedShareAcknowledgeRequest,
    pub(super) capture: DeadlineCapture,
}

/// Broker terminal retained until core settlement accepts the exact capability.
#[must_use = "a share acknowledgement terminal must settle exactly once"]
pub(super) struct ShareAcknowledgementTerminal {
    pub(super) attempt: ShareAcknowledgeAttempt,
    pub(super) acknowledgement: ShareAcknowledgement,
    pub(super) resolution: ShareAcknowledgeResolution,
    pub(super) route: ShareAcknowledgeRoute,
}

/// Lossless pre-submission rejection retaining the exact acknowledgement.
#[must_use = "a rejected acknowledgement still owns its exact capability"]
pub(super) struct ShareAcknowledgementPreparationFailure {
    pub(super) kind: ShareAcknowledgementPreparationFailureKind,
    pub(super) acknowledgement: ShareAcknowledgement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ShareAcknowledgementPreparationFailureKind {
    Core(ShareAcknowledgementApplyErrorKind),
    Protocol(ShareAcknowledgeRequestFailure),
    Rollback(ShareAcknowledgementApplyErrorKind),
}

impl ShareFetchSessionOwner {
    pub(super) fn prepare_acknowledgement(
        &mut self,
        acknowledgement: ShareAcknowledgement,
        capture: DeadlineCapture,
        now: kafka_client_core::Moment,
    ) -> Result<(), ShareAcknowledgementPreparationFailure> {
        if self.prepared_acknowledgement.is_some()
            || self.active_acknowledgement.is_some()
            || self.acknowledgement_terminal.is_some()
        {
            return Err(ShareAcknowledgementPreparationFailure {
                kind: ShareAcknowledgementPreparationFailureKind::Core(
                    ShareAcknowledgementApplyErrorKind::InvalidState,
                ),
                acknowledgement,
            });
        }
        let admission = self
            .machine
            .prepare_acknowledgement(acknowledgement, capture.deadline(), now)
            .map_err(|error| ShareAcknowledgementPreparationFailure {
                kind: ShareAcknowledgementPreparationFailureKind::Core(error.kind()),
                acknowledgement: error.into_acknowledgement(),
            })?;
        let (attempt, acknowledgement) = admission.into_parts();
        let request =
            match share_acknowledge_request(&self.group, &self.member, attempt, &acknowledgement) {
                Ok(request) => request,
                Err(kind) => {
                    return Err(self.rollback_preparation(attempt, acknowledgement, kind));
                }
            };
        self.prepared_acknowledgement = Some(PreparedShareAcknowledgement {
            attempt,
            acknowledgement,
            request,
            capture,
        });
        Ok(())
    }

    fn rollback_preparation(
        &mut self,
        attempt: ShareAcknowledgeAttempt,
        acknowledgement: ShareAcknowledgement,
        protocol: ShareAcknowledgeRequestFailure,
    ) -> ShareAcknowledgementPreparationFailure {
        match self.machine.settle_acknowledgement_failure(
            attempt,
            DeliveryStatus::NotSent,
            acknowledgement,
        ) {
            Ok(ShareAcknowledgementFailureSettlement::Retry(acknowledgement)) => {
                ShareAcknowledgementPreparationFailure {
                    kind: ShareAcknowledgementPreparationFailureKind::Protocol(protocol),
                    acknowledgement,
                }
            }
            Ok(ShareAcknowledgementFailureSettlement::Lost(_releases)) => {
                unreachable!("definitely-unsent preparation rollback cannot lose ownership")
            }
            Err(error) => ShareAcknowledgementPreparationFailure {
                kind: ShareAcknowledgementPreparationFailureKind::Rollback(error.kind()),
                acknowledgement: error.into_acknowledgement(),
            },
        }
    }
}

impl PreparedShareAcknowledgement {
    pub(super) const fn deadline(&self) -> kafka_client_core::Deadline {
        self.capture.deadline()
    }
}
