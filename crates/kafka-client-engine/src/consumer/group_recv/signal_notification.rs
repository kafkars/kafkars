//! Coalesced off-reactor publication for the global group receive signal.

use std::task::Waker;

use super::{GroupConsumerRecvSignal, GroupConsumerRecvWait, signal::GROUP_CONSUMER_RECV_CAPACITY};

impl GroupConsumerRecvSignal {
    pub(crate) fn prepare_notification(&self, wake: GroupConsumerRecvWait) -> bool {
        let mut state = self.lock();
        if !state.registrations.iter().any(|registration| {
            !registration.notified
                && (wake == GroupConsumerRecvWait::Change
                    || registration.wait == GroupConsumerRecvWait::Unlock)
        }) {
            return false;
        }
        match wake {
            GroupConsumerRecvWait::Change => state.change_queued = true,
            GroupConsumerRecvWait::Unlock => state.unlock_queued = true,
        }
        if state.notification_queued {
            return false;
        }
        state.notification_queued = true;
        true
    }

    pub(crate) fn restore_notification(&self) {
        let mut state = self.lock();
        state.notification_queued = false;
        state.change_queued = false;
        state.unlock_queued = false;
    }

    pub(crate) fn publish(&self) {
        let mut wakers: [Option<Waker>; GROUP_CONSUMER_RECV_CAPACITY] =
            std::array::from_fn(|_index| None);
        let wake_count = {
            let mut state = self.lock();
            if !state.notification_queued {
                return;
            }
            state.notification_queued = false;
            let change = std::mem::take(&mut state.change_queued);
            let unlock = std::mem::take(&mut state.unlock_queued);
            let mut wake_count = 0;
            for registration in &mut state.registrations {
                if registration.notified
                    || !(change || unlock && registration.wait == GroupConsumerRecvWait::Unlock)
                {
                    continue;
                }
                registration.notified = true;
                if let Some(waker) = registration.waker.take() {
                    wakers[wake_count] = Some(waker);
                    wake_count += 1;
                }
            }
            self.changed.notify_all();
            wake_count
        };
        for waker in wakers.into_iter().take(wake_count).flatten() {
            let _ignored = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waker.wake()));
        }
    }
}
