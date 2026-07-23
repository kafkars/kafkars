//! Single owner coordinating pending admission, cell claim, and resolution.

use crate::ProducerDeliveryObserver;

use super::{
    PendingAdmission, PendingCellError, PendingLocalFailure, PendingNotificationJob,
    PendingRecordTransferState, ProducerSendFailure, entry::PendingAdmissionFacts,
    promotion::PendingPromotion,
};

/// Misuse that retains every linear owner for an explicit recovery decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingAttemptStateError {
    RecordNotRetained,
    RecordNotDetached,
    RecordNotCommitted,
    Invariant,
    Cell(PendingCellError),
}

/// Sole owner of a removed admission and its cell's `Promoting` claim.
#[must_use = "resolve with accept, settle_local, or coordinated registry restore; drop never half-restores"]
pub(crate) struct PendingPromotionAttempt {
    pub(super) admission: Option<PendingAdmission>,
    pub(super) facts: Option<PendingAdmissionFacts>,
    pub(super) promotion: PendingPromotion,
    pub(super) transfer: PendingRecordTransferState,
}

impl PendingPromotionAttempt {
    pub(super) const fn new(admission: PendingAdmission, promotion: PendingPromotion) -> Self {
        Self {
            admission: Some(admission),
            facts: None,
            promotion,
            transfer: PendingRecordTransferState::Retained,
        }
    }

    /// Resolves a core-committed record into the accepted delivery observer.
    pub(crate) fn accept(
        self,
        observer: ProducerDeliveryObserver,
    ) -> Result<PendingAcceptedPromotion, PendingAttemptAcceptFailure> {
        if self.transfer != PendingRecordTransferState::Committed || self.facts.is_none() {
            return Err(PendingAttemptAcceptFailure::new(
                PendingAttemptStateError::RecordNotCommitted,
                self,
                observer,
            ));
        }
        let Self {
            admission: _,
            facts,
            promotion,
            transfer: _,
        } = self;
        let Some(facts) = facts else {
            return Err(PendingAttemptAcceptFailure::new(
                PendingAttemptStateError::Invariant,
                PendingPromotionAttempt {
                    admission: None,
                    facts: None,
                    promotion,
                    transfer: PendingRecordTransferState::Committed,
                },
                observer,
            ));
        };
        match promotion.accept(observer) {
            Ok(notification) => Ok(PendingAcceptedPromotion {
                facts,
                notification,
            }),
            Err((promotion, observer)) => Err(PendingAttemptAcceptFailure::new(
                PendingAttemptStateError::Cell(PendingCellError::AlreadySettled),
                PendingPromotionAttempt {
                    admission: None,
                    facts: Some(facts),
                    promotion,
                    transfer: PendingRecordTransferState::Committed,
                },
                observer,
            )),
        }
    }

    /// Settles an unadmitted retained record and creates its bounded signal.
    pub(crate) fn settle_local(
        self,
        failure: ProducerSendFailure,
    ) -> Result<PendingLocalFailure, PendingAttemptSettleFailure> {
        if self.transfer != PendingRecordTransferState::Retained || self.admission.is_none() {
            return Err(PendingAttemptSettleFailure::new(
                PendingAttemptStateError::RecordNotRetained,
                self,
                failure,
            ));
        }
        let Self {
            admission,
            facts: _,
            promotion,
            transfer: _,
        } = self;
        let Some(admission) = admission else {
            return Err(PendingAttemptSettleFailure::new(
                PendingAttemptStateError::Invariant,
                PendingPromotionAttempt {
                    admission: None,
                    facts: None,
                    promotion,
                    transfer: PendingRecordTransferState::Retained,
                },
                failure,
            ));
        };
        match promotion.settle_local(failure) {
            Ok(notification) => Ok(PendingLocalFailure::new(failure, admission, notification)),
            Err((promotion, error)) => Err(PendingAttemptSettleFailure::new(
                PendingAttemptStateError::Cell(error),
                PendingPromotionAttempt::new(admission, promotion),
                failure,
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn retained_admission_for_test(&self) -> Option<&PendingAdmission> {
        self.admission.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn cell_for_test(&self) -> std::sync::Arc<super::PendingSendCell> {
        if let Some(admission) = &self.admission {
            return admission.cell_for_test();
        }
        self.facts.as_ref().map_or_else(
            || panic!("promotion attempt should retain cell facts"),
            PendingAdmissionFacts::cell_for_test,
        )
    }
}

/// Accepted resolution retaining non-byte facts until notification handoff.
pub(crate) struct PendingAcceptedPromotion {
    facts: PendingAdmissionFacts,
    notification: PendingNotificationJob,
}

impl PendingAcceptedPromotion {
    pub(crate) fn into_notification(self) -> PendingNotificationJob {
        let Self {
            facts,
            notification,
        } = self;
        drop(facts);
        notification
    }
}

/// Failed accepted resolution retaining both the attempt and observer.
pub(crate) struct PendingAttemptAcceptFailure {
    error: PendingAttemptStateError,
    attempt: Box<PendingPromotionAttempt>,
    observer: ProducerDeliveryObserver,
}

impl PendingAttemptAcceptFailure {
    fn new(
        error: PendingAttemptStateError,
        attempt: PendingPromotionAttempt,
        observer: ProducerDeliveryObserver,
    ) -> Self {
        Self {
            error,
            attempt: Box::new(attempt),
            observer,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        PendingAttemptStateError,
        PendingPromotionAttempt,
        ProducerDeliveryObserver,
    ) {
        (self.error, *self.attempt, self.observer)
    }
}

/// Failed local resolution retaining the attempt and requested failure.
pub(crate) struct PendingAttemptSettleFailure {
    error: PendingAttemptStateError,
    attempt: Box<PendingPromotionAttempt>,
    failure: ProducerSendFailure,
}

impl PendingAttemptSettleFailure {
    fn new(
        error: PendingAttemptStateError,
        attempt: PendingPromotionAttempt,
        failure: ProducerSendFailure,
    ) -> Self {
        Self {
            error,
            attempt: Box::new(attempt),
            failure,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        PendingAttemptStateError,
        PendingPromotionAttempt,
        ProducerSendFailure,
    ) {
        (self.error, *self.attempt, self.failure)
    }
}
