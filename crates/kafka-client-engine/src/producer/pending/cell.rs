//! Linearized pending-send state shared by async and blocking observation.

use std::{
    fmt,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    task::{Context, Poll},
};

use crate::ProducerDeliveryObserver;

use super::{
    PendingNotificationJob, PendingPromotion, ProducerSendFailure,
    state::{
        DispatchOutcome, PendingSendPhase, abandon_phase, dispatch_phase, poll_phase,
        take_transition,
    },
};

pub(crate) enum PendingCellTransition {
    Accepted(ProducerDeliveryObserver),
    Ready(ProducerSendFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingCellError {
    Abandoned,
    AlreadyConsumed,
    TransitionInProgress,
    AlreadySettled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingDropOutcome {
    Unadmitted,
    PromotionWon,
    Accepted,
    LocallySettled,
    AlreadyDropped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PromotionRestore {
    Pending,
    Abandoned,
}

pub(crate) struct PendingSendCell {
    phase: Mutex<PendingSendPhase>,
    ready: Condvar,
}

impl fmt::Debug for PendingSendCell {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingSendCell")
            .finish_non_exhaustive()
    }
}

impl PendingSendCell {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            phase: Mutex::new(PendingSendPhase::Pending { waker: None }),
            ready: Condvar::new(),
        })
    }

    pub(crate) fn begin_promotion(self: &Arc<Self>) -> Result<PendingPromotion, PendingCellError> {
        let mut phase = self.lock();
        let previous = std::mem::replace(&mut *phase, PendingSendPhase::Consumed);
        match previous {
            PendingSendPhase::Pending { waker } => {
                *phase = PendingSendPhase::Promoting {
                    abandoned: false,
                    waker,
                };
                Ok(PendingPromotion::new(Arc::clone(self)))
            }
            PendingSendPhase::Abandoned => {
                *phase = PendingSendPhase::Abandoned;
                Err(PendingCellError::Abandoned)
            }
            PendingSendPhase::Promoting { abandoned, waker } => {
                *phase = PendingSendPhase::Promoting { abandoned, waker };
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

    pub(crate) fn poll(
        &self,
        context: &Context<'_>,
    ) -> Result<Poll<PendingCellTransition>, PendingCellError> {
        let mut phase = self.lock();
        poll_phase(&mut phase, context)
    }

    pub(crate) fn wait(&self) -> Result<PendingCellTransition, PendingCellError> {
        let mut phase = self.lock();
        loop {
            match take_transition(&mut phase) {
                Ok(Some(transition)) => return Ok(transition),
                Ok(None) => phase = self.wait_guard(phase),
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn abandon(&self) -> PendingDropOutcome {
        let (outcome, waker, observer) = abandon_phase(&mut self.lock());
        drop(waker);
        drop(observer);
        outcome
    }

    pub(super) fn accept_promotion(
        self: &Arc<Self>,
        observer: ProducerDeliveryObserver,
    ) -> Result<PendingNotificationJob, ProducerDeliveryObserver> {
        let mut phase = self.lock();
        let previous = std::mem::replace(&mut *phase, PendingSendPhase::Consumed);
        let PendingSendPhase::Promoting { abandoned, waker } = previous else {
            *phase = previous;
            return Err(observer);
        };
        *phase = PendingSendPhase::Accepted {
            abandoned,
            observer: Some(observer),
            waker,
        };
        self.ready.notify_all();
        Ok(PendingNotificationJob::new(Arc::clone(self)))
    }

    pub(super) fn settle_promotion(
        self: &Arc<Self>,
        failure: ProducerSendFailure,
    ) -> Result<PendingNotificationJob, PendingCellError> {
        let mut phase = self.lock();
        let previous = std::mem::replace(&mut *phase, PendingSendPhase::Consumed);
        let PendingSendPhase::Promoting { abandoned, waker } = previous else {
            *phase = previous;
            return Err(PendingCellError::AlreadySettled);
        };
        *phase = PendingSendPhase::Ready {
            abandoned,
            failure: Some(failure),
            waker,
        };
        self.ready.notify_all();
        Ok(PendingNotificationJob::new(Arc::clone(self)))
    }

    pub(super) fn restore_promotion(&self) -> Result<PromotionRestore, PendingCellError> {
        let mut phase = self.lock();
        let previous = std::mem::replace(&mut *phase, PendingSendPhase::Consumed);
        match previous {
            PendingSendPhase::Promoting {
                abandoned: false,
                waker,
            } => {
                *phase = PendingSendPhase::Pending { waker };
                Ok(PromotionRestore::Pending)
            }
            PendingSendPhase::Promoting {
                abandoned: true,
                waker,
            } => {
                drop(waker);
                *phase = PendingSendPhase::Abandoned;
                Ok(PromotionRestore::Abandoned)
            }
            other => {
                *phase = other;
                Err(PendingCellError::AlreadySettled)
            }
        }
    }

    pub(super) fn dispatch(&self) -> DispatchOutcome {
        dispatch_phase(&mut self.lock())
    }

    fn lock(&self) -> MutexGuard<'_, PendingSendPhase> {
        self.phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wait_guard<'a>(
        &self,
        phase: MutexGuard<'a, PendingSendPhase>,
    ) -> MutexGuard<'a, PendingSendPhase> {
        self.ready
            .wait(phase)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
