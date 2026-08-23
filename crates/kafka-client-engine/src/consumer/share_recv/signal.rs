//! Bounded multi-member waiter generations over one global share change signal.

use std::{
    sync::{Condvar, Mutex, MutexGuard},
    task::Waker,
};

use kafka_client_core::GroupId;

pub(super) const SHARE_CONSUMER_RECV_CAPACITY: usize = 8;

/// Exact generation of one active share receive registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShareConsumerRecvRegistration {
    pub(super) group_id: GroupId,
    generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareConsumerRecvWait {
    Change,
    Unlock,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareConsumerRecvSignalError {
    Full,
    GenerationExhausted,
    Stale,
}

pub(super) struct Registration {
    pub(super) id: ShareConsumerRecvRegistration,
    pub(super) wait: ShareConsumerRecvWait,
    pub(super) waker: Option<Waker>,
    pub(super) blocking: bool,
    pub(super) notified: bool,
}

pub(super) struct SignalState {
    next_generation: Option<u64>,
    pub(super) registrations: Vec<Registration>,
    pub(super) notification_queued: bool,
    pub(super) change_queued: bool,
    pub(super) unlock_queued: bool,
}

/// One bounded global wake domain; exact member probing remains authoritative.
pub(crate) struct ShareConsumerRecvSignal {
    state: Mutex<SignalState>,
    pub(super) changed: Condvar,
}

impl ShareConsumerRecvSignal {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(SignalState {
                next_generation: Some(1),
                registrations: Vec::with_capacity(SHARE_CONSUMER_RECV_CAPACITY),
                notification_queued: false,
                change_queued: false,
                unlock_queued: false,
            }),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn arm_task(
        &self,
        group_id: GroupId,
        current: Option<ShareConsumerRecvRegistration>,
        wait: ShareConsumerRecvWait,
        waker: &Waker,
    ) -> Result<ShareConsumerRecvRegistration, ShareConsumerRecvSignalError> {
        self.arm(group_id, current, wait, Some(waker), false, true)
    }

    pub(crate) fn rearm_task(
        &self,
        group_id: GroupId,
        current: ShareConsumerRecvRegistration,
        wait: ShareConsumerRecvWait,
        waker: &Waker,
    ) -> Result<ShareConsumerRecvRegistration, ShareConsumerRecvSignalError> {
        self.arm(group_id, Some(current), wait, Some(waker), false, false)
    }

    pub(crate) fn arm_blocking(
        &self,
        group_id: GroupId,
        current: Option<ShareConsumerRecvRegistration>,
        wait: ShareConsumerRecvWait,
    ) -> Result<ShareConsumerRecvRegistration, ShareConsumerRecvSignalError> {
        self.arm(group_id, current, wait, None, true, true)
    }

    pub(crate) fn rearm_blocking(
        &self,
        group_id: GroupId,
        current: ShareConsumerRecvRegistration,
        wait: ShareConsumerRecvWait,
    ) -> Result<ShareConsumerRecvRegistration, ShareConsumerRecvSignalError> {
        self.arm(group_id, Some(current), wait, None, true, false)
    }

    pub(crate) fn wait(
        &self,
        id: ShareConsumerRecvRegistration,
    ) -> Result<(), ShareConsumerRecvSignalError> {
        let mut state = self.lock();
        loop {
            let Some(registration) = state
                .registrations
                .iter_mut()
                .find(|registration| registration.id == id)
            else {
                return Err(ShareConsumerRecvSignalError::Stale);
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

    pub(crate) fn cancel(&self, id: ShareConsumerRecvRegistration) {
        let mut state = self.lock();
        if let Some(index) = state
            .registrations
            .iter()
            .position(|registration| registration.id == id)
        {
            state.registrations.swap_remove(index);
        }
    }

    #[cfg(test)]
    pub(crate) fn registration_count(&self) -> usize {
        self.lock().registrations.len()
    }

    fn arm(
        &self,
        group_id: GroupId,
        current: Option<ShareConsumerRecvRegistration>,
        wait: ShareConsumerRecvWait,
        waker: Option<&Waker>,
        blocking: bool,
        clear_notification: bool,
    ) -> Result<ShareConsumerRecvRegistration, ShareConsumerRecvSignalError> {
        let mut state = self.lock();
        if let Some(id) = current {
            let Some(registration) = state
                .registrations
                .iter_mut()
                .find(|registration| registration.id == id && id.group_id == group_id)
            else {
                return Err(ShareConsumerRecvSignalError::Stale);
            };
            registration.wait = wait;
            registration.blocking = blocking;
            if clear_notification {
                registration.notified = false;
            }
            update_waker(registration, waker);
            return Ok(id);
        }
        if state
            .registrations
            .iter()
            .any(|registration| registration.id.group_id == group_id)
        {
            return Err(ShareConsumerRecvSignalError::Stale);
        }
        if state.registrations.len() == SHARE_CONSUMER_RECV_CAPACITY {
            return Err(ShareConsumerRecvSignalError::Full);
        }
        let generation = state
            .next_generation
            .ok_or(ShareConsumerRecvSignalError::GenerationExhausted)?;
        state.next_generation = generation.checked_add(1);
        let id = ShareConsumerRecvRegistration {
            group_id,
            generation,
        };
        state.registrations.push(Registration {
            id,
            wait,
            waker: waker.cloned(),
            blocking,
            notified: false,
        });
        Ok(id)
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, SignalState> {
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
