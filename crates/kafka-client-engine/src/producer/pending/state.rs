//! Closed pending-send phase transitions under the pending cell's one lock.

use std::task::{Context, Poll, Waker};

use crate::ProducerDeliveryObserver;

use super::{
    PendingNotificationPermit, ProducerSendFailure,
    cell::{PendingCellError, PendingCellTransition, PendingDropOutcome},
};

pub(super) enum PendingSendPhase {
    Pending {
        permit: PendingNotificationPermit,
        waker: Option<Waker>,
    },
    Promoting {
        permit: PendingNotificationPermit,
        abandoned: bool,
        waker: Option<Waker>,
    },
    Accepted {
        abandoned: bool,
        observer: Option<ProducerDeliveryObserver>,
        waker: Option<Waker>,
    },
    Ready {
        abandoned: bool,
        failure: Option<ProducerSendFailure>,
        waker: Option<Waker>,
    },
    Abandoned,
    Consumed,
}

pub(crate) struct DispatchOutcome {
    pub(crate) waker: Option<Waker>,
    pub(crate) discarded: Option<ProducerDeliveryObserver>,
}

pub(super) fn poll_phase(
    phase: &mut PendingSendPhase,
    context: &Context<'_>,
) -> Result<Poll<PendingCellTransition>, PendingCellError> {
    match phase {
        PendingSendPhase::Pending { waker, .. }
        | PendingSendPhase::Promoting {
            abandoned: false,
            waker,
            ..
        } => {
            replace_waker(waker, context);
            Ok(Poll::Pending)
        }
        _ => take_transition(phase).map(|transition| transition.map_or(Poll::Pending, Poll::Ready)),
    }
}

pub(super) fn take_transition(
    phase: &mut PendingSendPhase,
) -> Result<Option<PendingCellTransition>, PendingCellError> {
    match phase {
        PendingSendPhase::Accepted {
            abandoned: false,
            observer,
            waker,
        } => {
            let Some(observer) = observer.take() else {
                return Err(PendingCellError::AlreadyConsumed);
            };
            drop(waker.take());
            *phase = PendingSendPhase::Consumed;
            Ok(Some(PendingCellTransition::Accepted(observer)))
        }
        PendingSendPhase::Ready {
            abandoned: false,
            failure,
            waker,
        } => {
            let Some(failure) = failure.take() else {
                return Err(PendingCellError::AlreadyConsumed);
            };
            drop(waker.take());
            *phase = PendingSendPhase::Consumed;
            Ok(Some(PendingCellTransition::Ready(failure)))
        }
        PendingSendPhase::Pending { .. }
        | PendingSendPhase::Promoting {
            abandoned: false, ..
        } => Ok(None),
        PendingSendPhase::Abandoned
        | PendingSendPhase::Promoting {
            abandoned: true, ..
        }
        | PendingSendPhase::Accepted {
            abandoned: true, ..
        }
        | PendingSendPhase::Ready {
            abandoned: true, ..
        } => Err(PendingCellError::Abandoned),
        PendingSendPhase::Consumed => Err(PendingCellError::AlreadyConsumed),
    }
}

pub(super) fn abandon_phase(
    phase: &mut PendingSendPhase,
) -> (
    PendingDropOutcome,
    Option<Waker>,
    Option<ProducerDeliveryObserver>,
    Option<PendingNotificationPermit>,
) {
    let previous = std::mem::replace(phase, PendingSendPhase::Consumed);
    match previous {
        PendingSendPhase::Pending { permit, waker } => {
            *phase = PendingSendPhase::Abandoned;
            (PendingDropOutcome::Unadmitted, waker, None, Some(permit))
        }
        PendingSendPhase::Promoting { permit, waker, .. } => {
            *phase = PendingSendPhase::Promoting {
                permit,
                abandoned: true,
                waker: None,
            };
            (PendingDropOutcome::PromotionWon, waker, None, None)
        }
        PendingSendPhase::Accepted {
            observer, waker, ..
        } => (PendingDropOutcome::Accepted, waker, observer, None),
        PendingSendPhase::Ready { waker, .. } => {
            (PendingDropOutcome::LocallySettled, waker, None, None)
        }
        PendingSendPhase::Abandoned | PendingSendPhase::Consumed => {
            *phase = previous;
            (PendingDropOutcome::AlreadyDropped, None, None, None)
        }
    }
}

pub(super) fn dispatch_phase(phase: &mut PendingSendPhase) -> DispatchOutcome {
    match phase {
        PendingSendPhase::Accepted {
            abandoned: false,
            waker,
            ..
        }
        | PendingSendPhase::Ready {
            abandoned: false,
            waker,
            ..
        } => DispatchOutcome {
            waker: waker.take(),
            discarded: None,
        },
        PendingSendPhase::Accepted {
            abandoned: true,
            observer,
            waker,
        } => {
            let outcome = DispatchOutcome {
                waker: waker.take(),
                discarded: observer.take(),
            };
            *phase = PendingSendPhase::Consumed;
            outcome
        }
        PendingSendPhase::Ready {
            abandoned: true,
            waker,
            ..
        } => {
            let outcome = DispatchOutcome {
                waker: waker.take(),
                discarded: None,
            };
            *phase = PendingSendPhase::Consumed;
            outcome
        }
        _ => DispatchOutcome {
            waker: None,
            discarded: None,
        },
    }
}

fn replace_waker(current: &mut Option<Waker>, context: &Context<'_>) {
    if current
        .as_ref()
        .is_none_or(|stored| !stored.will_wake(context.waker()))
    {
        *current = Some(context.waker().clone());
    }
}
