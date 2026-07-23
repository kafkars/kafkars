//! Lock-free shutdown, wake, and bounded-turn observations for one engine host.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::driver::ReactorWake;

#[derive(Debug)]
pub(crate) struct EngineHostControl {
    shutdown: AtomicBool,
    #[cfg(test)]
    failure: AtomicBool,
    #[cfg(test)]
    recovery_driver_released: AtomicBool,
    #[cfg(test)]
    pause_after_produce_admission: AtomicBool,
    #[cfg(test)]
    produce_admission_paused: AtomicBool,
    producer_turns: AtomicU64,
    driver_turns: AtomicU64,
    wake: ReactorWake,
}

impl EngineHostControl {
    pub(super) const fn new(wake: ReactorWake) -> Self {
        Self {
            shutdown: AtomicBool::new(false),
            #[cfg(test)]
            failure: AtomicBool::new(false),
            #[cfg(test)]
            recovery_driver_released: AtomicBool::new(false),
            #[cfg(test)]
            pause_after_produce_admission: AtomicBool::new(false),
            #[cfg(test)]
            produce_admission_paused: AtomicBool::new(false),
            producer_turns: AtomicU64::new(0),
            driver_turns: AtomicU64::new(0),
            wake,
        }
    }

    pub(super) fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        let _wake_result = self.wake.request();
    }

    pub(super) fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    pub(super) fn record_producer_turn(&self) {
        self.producer_turns.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_driver_turn(&self) {
        self.driver_turns.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn request_failure(&self) {
        self.failure.store(true, Ordering::Release);
        let _wake_result = self.wake.request();
    }

    #[cfg(test)]
    pub(crate) fn request_pause_after_produce_admission(&self) {
        self.pause_after_produce_admission
            .store(true, Ordering::Release);
        let _wake_result = self.wake.request();
    }

    #[cfg(test)]
    pub(super) fn await_failure_after_produce_admission(&self) -> bool {
        if !self.pause_after_produce_admission.load(Ordering::Acquire) {
            return false;
        }
        self.produce_admission_paused.store(true, Ordering::Release);
        while !self.failure.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        true
    }

    #[cfg(test)]
    pub(super) fn failure_requested(&self) -> bool {
        self.failure.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn record_recovery_driver_released(&self) {
        self.recovery_driver_released.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> EngineHostSnapshot {
        EngineHostSnapshot {
            producer_turns: self.producer_turns.load(Ordering::Relaxed),
            driver_turns: self.driver_turns.load(Ordering::Relaxed),
            recovery_driver_released: self.recovery_driver_released.load(Ordering::Acquire),
            produce_admission_paused: self.produce_admission_paused.load(Ordering::Acquire),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct EngineHostSnapshot {
    pub(crate) producer_turns: u64,
    pub(crate) driver_turns: u64,
    pub(crate) recovery_driver_released: bool,
    pub(crate) produce_admission_paused: bool,
}
