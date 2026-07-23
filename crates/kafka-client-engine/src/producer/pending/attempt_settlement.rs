//! Typed local and start-failure settlement for one pending promotion owner.

use super::{
    PendingAdmission, PendingAttemptStateError, PendingLocalFailure, PendingNotificationJob,
    PendingPromotionAttempt, PendingRecordTransferState, PendingStartFailure, ProducerSendFailure,
    ProducerSendReadyFailure,
};
use crate::ProducerSendStartFailure;

impl PendingPromotionAttempt {
    /// Settles ordinary unadmitted waiting and creates its bounded signal.
    pub(crate) fn settle_local(
        self,
        failure: ProducerSendFailure,
    ) -> Result<PendingLocalFailure, PendingAttemptSettleFailure<ProducerSendFailure>> {
        self.settle_ready(failure, ProducerSendReadyFailure::Local(failure))
            .map(|(pending, notification)| PendingLocalFailure::new(failure, pending, notification))
    }

    /// Settles a pre-core start failure without relabeling it as backpressure.
    pub(crate) fn settle_start(
        self,
        failure: ProducerSendStartFailure,
    ) -> Result<PendingStartFailure, PendingAttemptSettleFailure<ProducerSendStartFailure>> {
        self.settle_ready(failure, ProducerSendReadyFailure::Start(failure))
            .map(|(pending, notification)| PendingStartFailure::new(failure, pending, notification))
    }

    fn settle_ready<Failure: Copy>(
        self,
        requested: Failure,
        ready: ProducerSendReadyFailure,
    ) -> Result<(PendingAdmission, PendingNotificationJob), PendingAttemptSettleFailure<Failure>>
    {
        if self.transfer != PendingRecordTransferState::Retained || self.admission.is_none() {
            return Err(PendingAttemptSettleFailure::new(
                PendingAttemptStateError::RecordNotRetained,
                self,
                requested,
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
                requested,
            ));
        };
        match promotion.settle_ready(ready) {
            Ok(notification) => Ok((admission, notification)),
            Err((promotion, error)) => Err(PendingAttemptSettleFailure::new(
                PendingAttemptStateError::Cell(error),
                PendingPromotionAttempt::new(admission, promotion),
                requested,
            )),
        }
    }
}

/// Failed ready resolution retaining the attempt and exact requested failure.
pub(crate) struct PendingAttemptSettleFailure<Failure> {
    error: PendingAttemptStateError,
    attempt: Box<PendingPromotionAttempt>,
    failure: Failure,
}

impl<Failure> PendingAttemptSettleFailure<Failure> {
    fn new(
        error: PendingAttemptStateError,
        attempt: PendingPromotionAttempt,
        failure: Failure,
    ) -> Self {
        Self {
            error,
            attempt: Box::new(attempt),
            failure,
        }
    }

    pub(crate) fn into_parts(self) -> (PendingAttemptStateError, PendingPromotionAttempt, Failure) {
        (self.error, *self.attempt, self.failure)
    }
}
