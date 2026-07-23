//! Linearized pending-send state shared by async and blocking observation.

#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::{
    fmt,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    task::{Context, Poll},
};

use crate::ProducerDeliveryObserver;

use super::{
    PendingNotificationPermit, ProducerSendFailure,
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
    pub(super) phase: Mutex<PendingSendPhase>,
    pub(super) ready: Condvar,
    #[cfg(test)]
    pub(super) fail_next_restore: AtomicBool,
}

impl fmt::Debug for PendingSendCell {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingSendCell")
            .finish_non_exhaustive()
    }
}

impl PendingSendCell {
    pub(crate) fn new(permit: PendingNotificationPermit) -> Arc<Self> {
        Arc::new(Self {
            phase: Mutex::new(PendingSendPhase::Pending {
                permit,
                waker: None,
            }),
            ready: Condvar::new(),
            #[cfg(test)]
            fail_next_restore: AtomicBool::new(false),
        })
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
        let (outcome, waker, observer, permit) = abandon_phase(&mut self.lock());
        drop(waker);
        drop(observer);
        if let Some(permit) = permit {
            permit.release();
        }
        outcome
    }

    pub(super) fn is_abandoned(&self) -> bool {
        matches!(&*self.lock(), PendingSendPhase::Abandoned)
    }

    pub(super) fn dispatch(&self) -> DispatchOutcome {
        dispatch_phase(&mut self.lock())
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, PendingSendPhase> {
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
