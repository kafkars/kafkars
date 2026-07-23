//! One synchronized terminal cell shared by async and blocking observation.

mod lifecycle;

use std::{
    sync::{
        Condvar, Mutex, MutexGuard,
        mpsc::{SyncSender, TrySendError},
    },
    task::{Context, Poll, Waker},
};

use super::{
    CompletionId, CompletionObserverError,
    state::{CellPhase, Presence, take_terminal},
};

pub(super) struct StoreOutcome<T> {
    pub(super) waker: Option<Waker>,
    pub(super) discarded: Option<T>,
    pub(super) reclaim_after_drop: bool,
}

pub(super) struct CompletionCell<T> {
    slot: usize,
    phase: Mutex<CellPhase<T>>,
    ready: Condvar,
    reclaim: SyncSender<CompletionId>,
}

impl<T> CompletionCell<T> {
    pub(super) fn new(slot: usize, reclaim: SyncSender<CompletionId>) -> Self {
        Self {
            slot,
            phase: Mutex::new(CellPhase::Vacant { generation: 0 }),
            ready: Condvar::new(),
            reclaim,
        }
    }

    pub(super) fn store_terminal(&self, id: CompletionId, value: T) -> StoreOutcome<T> {
        let mut wake = None;
        let mut discarded = None;
        let mut reclaim_after_drop = false;
        {
            let mut phase = self.lock();
            let previous = std::mem::replace(&mut *phase, CellPhase::ReclaimPending { id });
            if let CellPhase::Pending {
                id: current,
                presence,
                waker,
            } = previous
            {
                if current == id {
                    *phase = CellPhase::Terminal {
                        id,
                        presence,
                        value: Some(value),
                    };
                    self.ready.notify_all();
                    if presence == Presence::Abandoned {
                        discarded = take_terminal(&mut phase);
                        *phase = CellPhase::ReclaimPending { id };
                        reclaim_after_drop = true;
                    } else {
                        wake = waker;
                    }
                } else {
                    *phase = CellPhase::Pending {
                        id: current,
                        presence,
                        waker,
                    };
                    discarded = Some(value);
                }
            } else {
                *phase = previous;
                discarded = Some(value);
            }
        }
        StoreOutcome {
            waker: wake,
            discarded,
            reclaim_after_drop,
        }
    }

    pub(super) fn poll(
        &self,
        id: CompletionId,
        context: &Context<'_>,
    ) -> Result<Poll<T>, CompletionObserverError> {
        let mut phase = self.lock();
        match &mut *phase {
            CellPhase::Pending {
                id: current,
                presence: Presence::Active,
                waker,
            } if *current == id => {
                if waker
                    .as_ref()
                    .is_none_or(|stored| !stored.will_wake(context.waker()))
                {
                    *waker = Some(context.waker().clone());
                }
                Ok(Poll::Pending)
            }
            CellPhase::Terminal {
                id: current,
                presence: Presence::Active,
                ..
            } if *current == id => take_ready(&mut phase, &self.reclaim).map(Poll::Ready),
            _ => Err(CompletionObserverError::Stale),
        }
    }

    pub(super) fn wait(&self, id: CompletionId) -> Result<T, CompletionObserverError> {
        let mut phase = self.lock();
        loop {
            match &*phase {
                CellPhase::Pending {
                    id: current,
                    presence: Presence::Active,
                    ..
                } if *current == id => phase = self.wait_guard(phase),
                CellPhase::Terminal {
                    id: current,
                    presence: Presence::Active,
                    ..
                } if *current == id => return take_ready(&mut phase, &self.reclaim),
                _ => return Err(CompletionObserverError::Stale),
            }
        }
    }

    pub(super) fn abandon(&self, id: CompletionId) {
        let (discarded, reclaim_after_drop) = {
            let mut phase = self.lock();
            match &mut *phase {
                CellPhase::Pending {
                    id: current,
                    presence,
                    waker,
                } if *current == id => {
                    *presence = Presence::Abandoned;
                    *waker = None;
                    (None, false)
                }
                CellPhase::Terminal {
                    id: current,
                    presence,
                    ..
                } if *current == id => {
                    *presence = Presence::Abandoned;
                    let value = take_terminal(&mut phase);
                    *phase = CellPhase::ReclaimPending { id };
                    (value, true)
                }
                _ => (None, false),
            }
        };
        drop(discarded);
        if reclaim_after_drop {
            self.queue_reclaim(id);
        }
    }

    pub(super) fn queue_reclaim(&self, id: CompletionId) {
        let mut phase = self.lock();
        if matches!(&*phase, CellPhase::ReclaimPending { id: current } if *current == id) {
            queue_reclaim(&mut phase, &self.reclaim);
        }
    }

    fn lock(&self) -> MutexGuard<'_, CellPhase<T>> {
        self.phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(super) fn lock_for_test(&self) -> MutexGuard<'_, CellPhase<T>> {
        self.lock()
    }

    fn wait_guard<'a>(&self, guard: MutexGuard<'a, CellPhase<T>>) -> MutexGuard<'a, CellPhase<T>> {
        self.ready
            .wait(guard)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn take_ready<T>(
    phase: &mut CellPhase<T>,
    reclaim: &SyncSender<CompletionId>,
) -> Result<T, CompletionObserverError> {
    let id = match phase {
        CellPhase::Terminal { id, .. } => *id,
        _ => return Err(CompletionObserverError::Stale),
    };
    let Some(value) = take_terminal(phase) else {
        return Err(CompletionObserverError::AlreadyObserved);
    };
    *phase = CellPhase::ReclaimPending { id };
    queue_reclaim(phase, reclaim);
    Ok(value)
}

fn queue_reclaim<T>(phase: &mut CellPhase<T>, sender: &SyncSender<CompletionId>) {
    let CellPhase::ReclaimPending { id } = *phase else {
        return;
    };
    match sender.try_send(id) {
        Ok(()) => *phase = CellPhase::ReclaimQueued { id },
        Err(TrySendError::Full(_id) | TrySendError::Disconnected(_id)) => {}
    }
}
