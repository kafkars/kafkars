//! Delivery-certain driver terminal interpretation and exact core settlement.

use kafka_client_core::{
    DeliveryStatus, ShareAcknowledgementApplyErrorKind, ShareAcknowledgementFailureSettlement,
};

use crate::driver::ShareAcknowledgeResolution;

use super::{
    super::{
        fetch_acknowledgement::ShareAcknowledgementTerminal, fetch_session::ShareFetchSessionOwner,
    },
    ShareAcknowledgementExecutionFailureKind, ShareAcknowledgementExecutionOutcome,
    ShareAcknowledgementExecutionPoll,
};

impl ShareFetchSessionOwner {
    pub(in crate::consumer::share) fn poll_acknowledgement(
        &mut self,
    ) -> Result<ShareAcknowledgementExecutionPoll, ShareAcknowledgementExecutionFailureKind> {
        if self.acknowledgement_terminal.is_some() {
            return Ok(ShareAcknowledgementExecutionPoll::Terminal);
        }
        let active = self.active_acknowledgement.as_mut().ok_or(
            ShareAcknowledgementExecutionFailureKind::Core(
                ShareAcknowledgementApplyErrorKind::InvalidState,
            ),
        )?;
        let Some(terminal) = active.call.try_terminal() else {
            return Ok(ShareAcknowledgementExecutionPoll::Pending);
        };
        let active = self
            .active_acknowledgement
            .take()
            .unwrap_or_else(|| unreachable!("polled acknowledgement remains active"));
        match terminal {
            Ok(raw) => {
                let (resolution, route) = raw.into_resolution();
                if route.broker_id() != active.attempt.fence().broker_id() {
                    route.accept();
                    self.retire_possibly_sent(active.attempt, active.acknowledgement)?;
                    self.acknowledgement_outcome =
                        Some(ShareAcknowledgementExecutionOutcome::Failed {
                            kind: ShareAcknowledgementExecutionFailureKind::BrokerMismatch,
                            delivery: DeliveryStatus::PossiblySent,
                            retry: None,
                        });
                    return Ok(ShareAcknowledgementExecutionPoll::Terminal);
                }
                self.acknowledgement_terminal = Some(ShareAcknowledgementTerminal {
                    attempt: active.attempt,
                    acknowledgement: active.acknowledgement,
                    resolution,
                    route,
                });
                Ok(ShareAcknowledgementExecutionPoll::Terminal)
            }
            Err(failure) => {
                let kind = failure.into_kind();
                self.retire_possibly_sent(active.attempt, active.acknowledgement)?;
                self.acknowledgement_outcome = Some(ShareAcknowledgementExecutionOutcome::Failed {
                    kind: ShareAcknowledgementExecutionFailureKind::Completion(kind),
                    delivery: DeliveryStatus::PossiblySent,
                    retry: None,
                });
                Ok(ShareAcknowledgementExecutionPoll::Terminal)
            }
        }
    }

    pub(in crate::consumer::share) fn settle_acknowledgement_terminal(
        &mut self,
    ) -> Result<ShareAcknowledgementExecutionOutcome, ShareAcknowledgementExecutionFailureKind>
    {
        let terminal = self.acknowledgement_terminal.take().ok_or(
            ShareAcknowledgementExecutionFailureKind::Core(
                ShareAcknowledgementApplyErrorKind::InvalidState,
            ),
        )?;
        let ShareAcknowledgementTerminal {
            attempt,
            acknowledgement,
            resolution,
            route,
        } = terminal;
        let settlement = match &resolution {
            ShareAcknowledgeResolution::Succeeded(success)
                if success
                    .outcomes
                    .iter()
                    .all(|outcome| outcome.error_code.is_none()) =>
            {
                self.machine
                    .settle_acknowledged(attempt, acknowledgement)
                    .map(|releases| {
                        drop(releases);
                        None
                    })
            }
            ShareAcknowledgeResolution::Succeeded(_)
            | ShareAcknowledgeResolution::BrokerRejected(_) => self
                .machine
                .settle_acknowledgement_failure(
                    attempt,
                    DeliveryStatus::PossiblySent,
                    acknowledgement,
                )
                .map(settlement_retry),
            ShareAcknowledgeResolution::Failed { delivery, .. } => self
                .machine
                .settle_acknowledgement_failure(attempt, *delivery, acknowledgement)
                .map(settlement_retry),
        };
        let retry = match settlement {
            Ok(retry) => retry,
            Err(error) => {
                let kind = error.kind();
                self.acknowledgement_terminal = Some(ShareAcknowledgementTerminal {
                    attempt,
                    acknowledgement: error.into_acknowledgement(),
                    resolution,
                    route,
                });
                return Err(ShareAcknowledgementExecutionFailureKind::Core(kind));
            }
        };
        route.accept();
        match resolution {
            ShareAcknowledgeResolution::Failed { kind, delivery } => {
                Ok(ShareAcknowledgementExecutionOutcome::Failed {
                    kind: ShareAcknowledgementExecutionFailureKind::Driver(kind),
                    delivery,
                    retry,
                })
            }
            response => Ok(ShareAcknowledgementExecutionOutcome::Responded(response)),
        }
    }
}

fn settlement_retry(
    settlement: ShareAcknowledgementFailureSettlement,
) -> Option<kafka_client_core::ShareAcknowledgement> {
    match settlement {
        ShareAcknowledgementFailureSettlement::Lost(releases) => {
            drop(releases);
            None
        }
        ShareAcknowledgementFailureSettlement::Retry(acknowledgement) => Some(acknowledgement),
    }
}
