//! Atomic pending-cell promotion claims and terminal transition installation.

use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::Ordering;

use crate::ProducerDeliveryObserver;

use super::{
    PendingCellError, PendingNotificationJob, PendingSendCell, ProducerSendFailure,
    cell::PromotionRestore, promotion::PendingPromotion, state::PendingSendPhase,
};

impl PendingSendCell {
    pub(super) fn begin_promotion(self: &Arc<Self>) -> Result<PendingPromotion, PendingCellError> {
        let mut phase = self.lock();
        let previous = std::mem::replace(&mut *phase, PendingSendPhase::Consumed);
        match previous {
            PendingSendPhase::Pending { permit, waker } => {
                *phase = PendingSendPhase::Promoting {
                    permit,
                    abandoned: false,
                    waker,
                };
                Ok(PendingPromotion::new(Arc::clone(self)))
            }
            PendingSendPhase::Abandoned => {
                *phase = PendingSendPhase::Abandoned;
                Err(PendingCellError::Abandoned)
            }
            PendingSendPhase::Promoting {
                permit,
                abandoned,
                waker,
            } => {
                *phase = PendingSendPhase::Promoting {
                    permit,
                    abandoned,
                    waker,
                };
                Err(PendingCellError::TransitionInProgress)
            }
            PendingSendPhase::Accepted {
                abandoned,
                observer,
                waker,
            } => {
                *phase = PendingSendPhase::Accepted {
                    abandoned,
                    observer,
                    waker,
                };
                Err(PendingCellError::AlreadySettled)
            }
            PendingSendPhase::Ready {
                abandoned,
                failure,
                waker,
            } => {
                *phase = PendingSendPhase::Ready {
                    abandoned,
                    failure,
                    waker,
                };
                Err(PendingCellError::AlreadySettled)
            }
            PendingSendPhase::Consumed => {
                *phase = PendingSendPhase::Consumed;
                Err(PendingCellError::AlreadyConsumed)
            }
        }
    }

    #[cfg(test)]
    pub(super) fn begin_promotion_for_test(
        self: &Arc<Self>,
    ) -> Result<PendingPromotion, PendingCellError> {
        self.begin_promotion()
    }

    #[cfg(test)]
    pub(crate) fn settle_local_for_test(
        self: &Arc<Self>,
        failure: ProducerSendFailure,
    ) -> Result<PendingNotificationJob, PendingCellError> {
        let promotion = self.begin_promotion()?;
        promotion
            .settle_local(failure)
            .map_err(|(_promotion, error)| error)
    }

    pub(super) fn accept_promotion(
        self: &Arc<Self>,
        observer: ProducerDeliveryObserver,
    ) -> Result<PendingNotificationJob, ProducerDeliveryObserver> {
        let mut phase = self.lock();
        let previous = std::mem::replace(&mut *phase, PendingSendPhase::Consumed);
        let PendingSendPhase::Promoting {
            permit,
            abandoned,
            waker,
        } = previous
        else {
            *phase = previous;
            return Err(observer);
        };
        *phase = PendingSendPhase::Accepted {
            abandoned,
            observer: Some(observer),
            waker,
        };
        self.ready.notify_all();
        Ok(PendingNotificationJob::new(Arc::clone(self), permit))
    }

    pub(super) fn settle_promotion(
        self: &Arc<Self>,
        failure: ProducerSendFailure,
    ) -> Result<PendingNotificationJob, PendingCellError> {
        let mut phase = self.lock();
        let previous = std::mem::replace(&mut *phase, PendingSendPhase::Consumed);
        let PendingSendPhase::Promoting {
            permit,
            abandoned,
            waker,
        } = previous
        else {
            *phase = previous;
            return Err(PendingCellError::AlreadySettled);
        };
        *phase = PendingSendPhase::Ready {
            abandoned,
            failure: Some(failure),
            waker,
        };
        self.ready.notify_all();
        Ok(PendingNotificationJob::new(Arc::clone(self), permit))
    }

    pub(super) fn restore_promotion(&self) -> Result<PromotionRestore, PendingCellError> {
        #[cfg(test)]
        if self.fail_next_restore.swap(false, Ordering::AcqRel) {
            return Err(PendingCellError::AlreadySettled);
        }
        let mut phase = self.lock();
        let previous = std::mem::replace(&mut *phase, PendingSendPhase::Consumed);
        match previous {
            PendingSendPhase::Promoting {
                permit,
                abandoned: false,
                waker,
            } => {
                *phase = PendingSendPhase::Pending { permit, waker };
                Ok(PromotionRestore::Pending)
            }
            PendingSendPhase::Promoting {
                permit,
                abandoned: true,
                waker,
            } => {
                drop(waker);
                *phase = PendingSendPhase::Abandoned;
                permit.release();
                Ok(PromotionRestore::Abandoned)
            }
            other => {
                *phase = other;
                Err(PendingCellError::AlreadySettled)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn inject_restore_failure_for_test(&self) {
        self.fail_next_restore.store(true, Ordering::Release);
    }
}
