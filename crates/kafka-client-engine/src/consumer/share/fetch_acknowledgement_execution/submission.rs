//! Exact prepared acknowledgement handoff to the broker-routed driver call.

use kafka_client_core::{DeliveryStatus, ShareAcknowledgementApplyErrorKind};

use crate::driver::{DriverOwner, ShareAcknowledgeCall, ShareAcknowledgeDriverSubmitErrorKind};

use super::{
    super::{
        fetch_acknowledgement::PreparedShareAcknowledgement, fetch_session::ShareFetchSessionOwner,
    },
    ActiveShareAcknowledgementCall, ShareAcknowledgementExecutionFailureKind,
    ShareAcknowledgementExecutionOutcome, ShareAcknowledgementSubmissionTurn,
};

impl ShareFetchSessionOwner {
    pub(in crate::consumer::share) fn submit_prepared_acknowledgement(
        &mut self,
        driver: &DriverOwner,
        now: kafka_client_core::Moment,
    ) -> Result<ShareAcknowledgementSubmissionTurn, ShareAcknowledgementExecutionFailureKind> {
        if self.active_acknowledgement.is_some() || self.acknowledgement_terminal.is_some() {
            return Err(ShareAcknowledgementExecutionFailureKind::Core(
                ShareAcknowledgementApplyErrorKind::InvalidState,
            ));
        }
        let prepared = self.prepared_acknowledgement.take().ok_or(
            ShareAcknowledgementExecutionFailureKind::Core(
                ShareAcknowledgementApplyErrorKind::InvalidState,
            ),
        )?;
        let PreparedShareAcknowledgement {
            attempt,
            acknowledgement,
            request,
            capture,
        } = prepared;
        match ShareAcknowledgeCall::submit(
            driver,
            attempt.fence().broker_id(),
            request,
            capture.operation_deadline(),
        ) {
            Ok(call) => {
                self.active_acknowledgement = Some(ActiveShareAcknowledgementCall {
                    attempt,
                    acknowledgement,
                    call,
                });
                Ok(ShareAcknowledgementSubmissionTurn::Submitted)
            }
            Err(failure) => {
                let kind = failure.kind();
                failure.discard();
                let acknowledgement = self.restore_not_sent(attempt, acknowledgement)?;
                if kind == ShareAcknowledgeDriverSubmitErrorKind::Full {
                    match self.prepare_acknowledgement(acknowledgement, capture, now) {
                        Ok(()) => Ok(ShareAcknowledgementSubmissionTurn::Backpressured),
                        Err(failure) => {
                            self.acknowledgement_outcome =
                                Some(ShareAcknowledgementExecutionOutcome::Failed {
                                    kind: ShareAcknowledgementExecutionFailureKind::Preparation(
                                        failure.kind,
                                    ),
                                    delivery: DeliveryStatus::NotSent,
                                    retry: Some(failure.acknowledgement),
                                });
                            Ok(ShareAcknowledgementSubmissionTurn::Terminal)
                        }
                    }
                } else {
                    self.acknowledgement_outcome =
                        Some(ShareAcknowledgementExecutionOutcome::Failed {
                            kind: ShareAcknowledgementExecutionFailureKind::Submit(kind),
                            delivery: DeliveryStatus::NotSent,
                            retry: Some(acknowledgement),
                        });
                    Ok(ShareAcknowledgementSubmissionTurn::Terminal)
                }
            }
        }
    }
}
