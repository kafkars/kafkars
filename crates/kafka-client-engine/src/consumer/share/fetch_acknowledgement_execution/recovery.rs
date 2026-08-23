//! Lossless shutdown, abandonment, and internal ownership-fault retention.

use kafka_client_core::{
    DeliveryStatus, ShareAcknowledgeAttempt, ShareAcknowledgement,
    ShareAcknowledgementApplyErrorKind, ShareAcknowledgementFailureSettlement,
};

use crate::driver::share_acknowledge::ShareAcknowledgeCompletionErrorKind;

use super::{
    super::fetch_session::ShareFetchSessionOwner, ShareAcknowledgementExecutionFailureKind,
    ShareAcknowledgementExecutionOutcome, ShareAcknowledgementOwnershipFault,
};

impl ShareFetchSessionOwner {
    pub(in crate::consumer::share) fn recover_acknowledgement_after_driver_shutdown(
        &mut self,
    ) -> Result<bool, ShareAcknowledgementExecutionFailureKind> {
        let Some(active) = self.active_acknowledgement.take() else {
            return Ok(false);
        };
        active.call.recover_after_driver_shutdown().discard();
        self.retire_possibly_sent(active.attempt, active.acknowledgement)?;
        self.acknowledgement_outcome = Some(ShareAcknowledgementExecutionOutcome::Failed {
            kind: ShareAcknowledgementExecutionFailureKind::Completion(
                ShareAcknowledgeCompletionErrorKind::Closed,
            ),
            delivery: DeliveryStatus::PossiblySent,
            retry: None,
        });
        Ok(true)
    }

    pub(in crate::consumer::share) fn recover_prepared_acknowledgement_after_driver_shutdown(
        &mut self,
    ) -> Result<bool, ShareAcknowledgementExecutionFailureKind> {
        let Some(prepared) = self.prepared_acknowledgement.take() else {
            return Ok(false);
        };
        let acknowledgement = self.restore_not_sent(prepared.attempt, prepared.acknowledgement)?;
        drop(prepared.request);
        self.abandon_retry_acknowledgement(acknowledgement)?;
        self.acknowledgement_outcome = Some(ShareAcknowledgementExecutionOutcome::Failed {
            kind: ShareAcknowledgementExecutionFailureKind::Completion(
                ShareAcknowledgeCompletionErrorKind::Closed,
            ),
            delivery: DeliveryStatus::NotSent,
            retry: None,
        });
        Ok(true)
    }

    pub(in crate::consumer::share) fn retain_settled_acknowledgement(
        &mut self,
        outcome: ShareAcknowledgementExecutionOutcome,
    ) -> Result<(), ShareAcknowledgementExecutionOutcome> {
        if self.acknowledgement_outcome.is_some() {
            return Err(outcome);
        }
        self.acknowledgement_outcome = Some(outcome);
        Ok(())
    }

    pub(in crate::consumer::share) fn take_acknowledgement_outcome(
        &mut self,
    ) -> Option<ShareAcknowledgementExecutionOutcome> {
        self.acknowledgement_outcome.take()
    }

    pub(in crate::consumer::share) fn abandon_acknowledgement_outcome(
        &mut self,
    ) -> Result<bool, ShareAcknowledgementExecutionFailureKind> {
        let Some(outcome) = self.take_acknowledgement_outcome() else {
            return Ok(false);
        };
        if let ShareAcknowledgementExecutionOutcome::Failed {
            retry: Some(acknowledgement),
            ..
        } = outcome
        {
            self.abandon_retry_acknowledgement(acknowledgement)?;
        }
        Ok(true)
    }

    pub(in crate::consumer::share) fn abandon_prepared_acknowledgement(
        &mut self,
    ) -> Result<bool, ShareAcknowledgementExecutionFailureKind> {
        let Some(prepared) = self.prepared_acknowledgement.take() else {
            return Ok(false);
        };
        let acknowledgement = self.restore_not_sent(prepared.attempt, prepared.acknowledgement)?;
        drop(prepared.request);
        self.abandon_retry_acknowledgement(acknowledgement)?;
        Ok(true)
    }

    pub(super) fn restore_not_sent(
        &mut self,
        attempt: ShareAcknowledgeAttempt,
        acknowledgement: ShareAcknowledgement,
    ) -> Result<ShareAcknowledgement, ShareAcknowledgementExecutionFailureKind> {
        match self.machine.settle_acknowledgement_failure(
            attempt,
            DeliveryStatus::NotSent,
            acknowledgement,
        ) {
            Ok(ShareAcknowledgementFailureSettlement::Retry(acknowledgement)) => {
                Ok(acknowledgement)
            }
            Ok(ShareAcknowledgementFailureSettlement::Lost(_releases)) => {
                unreachable!("NotSent acknowledgement cannot lose ownership")
            }
            Err(error) => {
                let kind = error.kind();
                self.retain_acknowledgement_fault(ShareAcknowledgementOwnershipFault::Apply(error));
                Err(ShareAcknowledgementExecutionFailureKind::Core(kind))
            }
        }
    }

    pub(super) fn retire_possibly_sent(
        &mut self,
        attempt: ShareAcknowledgeAttempt,
        acknowledgement: ShareAcknowledgement,
    ) -> Result<(), ShareAcknowledgementExecutionFailureKind> {
        match self.machine.settle_acknowledgement_failure(
            attempt,
            DeliveryStatus::PossiblySent,
            acknowledgement,
        ) {
            Ok(ShareAcknowledgementFailureSettlement::Lost(releases)) => {
                drop(releases);
                Ok(())
            }
            Ok(ShareAcknowledgementFailureSettlement::Retry(_acknowledgement)) => {
                unreachable!("PossiblySent acknowledgement cannot regain retry ownership")
            }
            Err(error) => {
                let kind = error.kind();
                self.retain_acknowledgement_fault(ShareAcknowledgementOwnershipFault::Apply(error));
                Err(ShareAcknowledgementExecutionFailureKind::Core(kind))
            }
        }
    }

    fn abandon_retry_acknowledgement(
        &mut self,
        acknowledgement: ShareAcknowledgement,
    ) -> Result<(), ShareAcknowledgementExecutionFailureKind> {
        let (acquisitions, batches) = acknowledgement.into_parts();
        match self.machine.ledger_mut().abandon_batch(acquisitions) {
            Ok(releases) => {
                drop(releases);
                drop(batches);
                Ok(())
            }
            Err(error) => {
                let kind = error.kind();
                self.retain_acknowledgement_fault(ShareAcknowledgementOwnershipFault::Abandon {
                    error,
                    batches,
                });
                Err(ShareAcknowledgementExecutionFailureKind::Core(
                    ShareAcknowledgementApplyErrorKind::Acquisition(kind),
                ))
            }
        }
    }

    fn retain_acknowledgement_fault(&mut self, fault: ShareAcknowledgementOwnershipFault) {
        self.acknowledgement_faults.push(fault);
    }
}
