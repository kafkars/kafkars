//! Single-flight session admission and delivery-certain acknowledgement settlement.

use crate::{Deadline, DeliveryStatus, Moment};

use super::{
    ShareAcknowledgeAttempt, ShareAcknowledgement, ShareAcknowledgementAdmission,
    ShareAcknowledgementApplyError, ShareAcknowledgementApplyErrorKind as ErrorKind,
    ShareAcknowledgementFailureSettlement, ShareAcquisitionRelease, ShareFetchSessionMachine,
    ShareFetchSessionPhase,
};

impl ShareFetchSessionMachine {
    /// Returns the sole in-flight fetch attempt, if any.
    pub const fn in_flight(&self) -> Option<super::ShareFetchAttempt> {
        self.in_flight
    }

    /// Returns the sole acknowledgement attempt, if any.
    pub const fn acknowledging(&self) -> Option<ShareAcknowledgeAttempt> {
        self.acknowledging
    }

    /// Admits one exact acknowledgement under the current broker-session epoch.
    pub fn prepare_acknowledgement(
        &mut self,
        acknowledgement: ShareAcknowledgement,
        deadline: Deadline,
        now: Moment,
    ) -> Result<ShareAcknowledgementAdmission, ShareAcknowledgementApplyError> {
        let result = self.preflight_acknowledgement(&acknowledgement, deadline, now);
        let attempt = match result {
            Ok(attempt) => attempt,
            Err(kind) => return Err(ShareAcknowledgementApplyError::new(kind, acknowledgement)),
        };
        if let Err(kind) = self
            .ledger_mut()
            .begin_acknowledgement(&acknowledgement, now)
        {
            return Err(ShareAcknowledgementApplyError::new(
                ErrorKind::Acquisition(kind),
                acknowledgement,
            ));
        }
        self.acknowledging = Some(attempt);
        self.phase = ShareFetchSessionPhase::Acknowledging;
        Ok(ShareAcknowledgementAdmission::new(attempt, acknowledgement))
    }

    /// Applies one exactly correlated successful response and retires local ownership.
    pub fn settle_acknowledged(
        &mut self,
        attempt: ShareAcknowledgeAttempt,
        acknowledgement: ShareAcknowledgement,
    ) -> Result<Vec<ShareAcquisitionRelease>, ShareAcknowledgementApplyError> {
        if let Err(kind) = self.validate_acknowledgement_attempt(attempt, &acknowledgement) {
            return Err(ShareAcknowledgementApplyError::new(kind, acknowledgement));
        }
        let Some(next_fence) = attempt.fence().next_session() else {
            return Err(ShareAcknowledgementApplyError::new(
                ErrorKind::SessionEpochExhausted,
                acknowledgement,
            ));
        };
        let releases = match self.ledger_mut().retire_acknowledgement(&acknowledgement) {
            Ok(releases) => releases,
            Err(kind) => {
                return Err(ShareAcknowledgementApplyError::new(
                    ErrorKind::Acquisition(kind),
                    acknowledgement,
                ));
            }
        };
        self.fence = next_fence;
        self.acknowledging = None;
        self.phase = ShareFetchSessionPhase::Ready;
        Ok(releases)
    }

    /// Applies an authoritative transport failure without strengthening certainty.
    pub fn settle_acknowledgement_failure(
        &mut self,
        attempt: ShareAcknowledgeAttempt,
        delivery: DeliveryStatus,
        acknowledgement: ShareAcknowledgement,
    ) -> Result<ShareAcknowledgementFailureSettlement, ShareAcknowledgementApplyError> {
        if let Err(kind) = self.validate_acknowledgement_attempt(attempt, &acknowledgement) {
            return Err(ShareAcknowledgementApplyError::new(kind, acknowledgement));
        }
        match delivery {
            DeliveryStatus::NotSent => {
                if let Err(kind) = self.ledger_mut().restore_acknowledgement(&acknowledgement) {
                    return Err(ShareAcknowledgementApplyError::new(
                        ErrorKind::Acquisition(kind),
                        acknowledgement,
                    ));
                }
                self.acknowledging = None;
                self.phase = ShareFetchSessionPhase::Ready;
                Ok(ShareAcknowledgementFailureSettlement::Retry(
                    acknowledgement,
                ))
            }
            DeliveryStatus::PossiblySent => {
                let releases = match self.ledger_mut().retire_acknowledgement(&acknowledgement) {
                    Ok(releases) => releases,
                    Err(kind) => {
                        return Err(ShareAcknowledgementApplyError::new(
                            ErrorKind::Acquisition(kind),
                            acknowledgement,
                        ));
                    }
                };
                self.acknowledging = None;
                self.phase = ShareFetchSessionPhase::Lost;
                Ok(ShareAcknowledgementFailureSettlement::Lost(releases))
            }
        }
    }

    fn preflight_acknowledgement(
        &self,
        acknowledgement: &ShareAcknowledgement,
        deadline: Deadline,
        now: Moment,
    ) -> Result<ShareAcknowledgeAttempt, ErrorKind> {
        if self.phase() != ShareFetchSessionPhase::Ready
            || self.in_flight().is_some()
            || self.acknowledging.is_some()
        {
            return Err(ErrorKind::InvalidState);
        }
        if deadline.is_elapsed_at(now) {
            return Err(ErrorKind::DeadlineElapsed);
        }
        if acknowledgement.fence().next_session() != Some(self.fence()) {
            return Err(ErrorKind::SessionMismatch);
        }
        if self.fence().next_session().is_none() {
            return Err(ErrorKind::SessionEpochExhausted);
        }
        Ok(ShareAcknowledgeAttempt::new(
            self.fence(),
            acknowledgement.fence(),
            deadline,
        ))
    }

    fn validate_acknowledgement_attempt(
        &self,
        attempt: ShareAcknowledgeAttempt,
        acknowledgement: &ShareAcknowledgement,
    ) -> Result<(), ErrorKind> {
        if self.phase() != ShareFetchSessionPhase::Acknowledging
            || self.acknowledging != Some(attempt)
        {
            return Err(ErrorKind::StaleAttempt);
        }
        if attempt.fence() != self.fence() || attempt.acquisition_fence() != acknowledgement.fence()
        {
            return Err(ErrorKind::SessionMismatch);
        }
        Ok(())
    }
}
