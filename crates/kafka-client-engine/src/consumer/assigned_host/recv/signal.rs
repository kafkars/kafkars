//! One generation-fenced waiter shared by task and blocking observation.

use std::{
    sync::{Condvar, Mutex, MutexGuard},
    task::Waker,
};

/// Exact generation of one active receive registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AssignedConsumerRecvRegistration(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerRecvWait {
    Change,
    Unlock,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerRecvSignalError {
    GenerationExhausted,
    Stale,
}

struct Registration {
    id: AssignedConsumerRecvRegistration,
    wait: AssignedConsumerRecvWait,
    waker: Option<Waker>,
    blocking: bool,
    notified: bool,
}

struct SignalState {
    next_generation: Option<u64>,
    registration: Option<Registration>,
    notification_queued: bool,
}

/// Capacity-one notification state; it never owns a Fetch lease or deadline.
pub(crate) struct AssignedConsumerRecvSignal {
    state: Mutex<SignalState>,
    changed: Condvar,
}

impl AssignedConsumerRecvSignal {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(SignalState {
                next_generation: Some(1),
                registration: None,
                notification_queued: false,
            }),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn arm_task(
        &self,
        current: Option<AssignedConsumerRecvRegistration>,
        wait: AssignedConsumerRecvWait,
        waker: &Waker,
    ) -> Result<AssignedConsumerRecvRegistration, AssignedConsumerRecvSignalError> {
        self.arm(current, wait, Some(waker), false)
    }

    pub(super) fn arm_blocking(
        &self,
        current: Option<AssignedConsumerRecvRegistration>,
    ) -> Result<AssignedConsumerRecvRegistration, AssignedConsumerRecvSignalError> {
        self.arm(current, AssignedConsumerRecvWait::Change, None, true)
    }

    pub(crate) fn prepare_notification(&self, wake: AssignedConsumerRecvWait) -> bool {
        let mut state = self.lock();
        if state.notification_queued {
            return false;
        }
        let Some(registration) = state.registration.as_ref() else {
            return false;
        };
        if registration.notified
            || (registration.waker.is_none() && !registration.blocking)
            || (wake == AssignedConsumerRecvWait::Unlock
                && registration.wait != AssignedConsumerRecvWait::Unlock)
        {
            return false;
        }
        state.notification_queued = true;
        true
    }

    pub(super) fn restore_notification(&self) {
        self.lock().notification_queued = false;
    }

    pub(super) fn publish(&self) {
        let waker = {
            let mut state = self.lock();
            if !state.notification_queued {
                return;
            }
            state.notification_queued = false;
            let Some(registration) = state.registration.as_mut() else {
                return;
            };
            registration.notified = true;
            let waker = registration.waker.take();
            self.changed.notify_all();
            waker
        };
        if let Some(waker) = waker {
            let _ignored = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waker.wake()));
        }
    }

    pub(super) fn wait(
        &self,
        id: AssignedConsumerRecvRegistration,
    ) -> Result<(), AssignedConsumerRecvSignalError> {
        let mut state = self.lock();
        loop {
            let Some(registration) = state
                .registration
                .as_mut()
                .filter(|registration| registration.id == id)
            else {
                return Err(AssignedConsumerRecvSignalError::Stale);
            };
            if registration.notified {
                registration.notified = false;
                return Ok(());
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(crate) fn cancel(&self, id: AssignedConsumerRecvRegistration) {
        let mut state = self.lock();
        if state
            .registration
            .as_ref()
            .is_some_and(|registration| registration.id == id)
        {
            state.registration = None;
        }
    }

    fn arm(
        &self,
        current: Option<AssignedConsumerRecvRegistration>,
        wait: AssignedConsumerRecvWait,
        waker: Option<&Waker>,
        blocking: bool,
    ) -> Result<AssignedConsumerRecvRegistration, AssignedConsumerRecvSignalError> {
        let mut state = self.lock();
        let id = if let Some(id) = current {
            id
        } else {
            let generation = state
                .next_generation
                .ok_or(AssignedConsumerRecvSignalError::GenerationExhausted)?;
            state.next_generation = generation.checked_add(1);
            AssignedConsumerRecvRegistration(generation)
        };
        match state.registration.as_mut() {
            Some(registration) if registration.id == id => {
                registration.wait = wait;
                registration.blocking = blocking;
                registration.notified = false;
                update_waker(registration, waker);
            }
            Some(_) if current.is_some() => return Err(AssignedConsumerRecvSignalError::Stale),
            Some(_) => return Err(AssignedConsumerRecvSignalError::Stale),
            None => {
                state.registration = Some(Registration {
                    id,
                    wait,
                    waker: waker.cloned(),
                    blocking,
                    notified: false,
                });
            }
        }
        Ok(id)
    }

    fn lock(&self) -> MutexGuard<'_, SignalState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn update_waker(registration: &mut Registration, waker: Option<&Waker>) {
    let Some(waker) = waker else {
        registration.waker = None;
        return;
    };
    if registration
        .waker
        .as_ref()
        .is_none_or(|present| !present.will_wake(waker))
    {
        registration.waker = Some(waker.clone());
    }
}
