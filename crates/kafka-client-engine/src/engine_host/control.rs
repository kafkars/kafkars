//! Lock-free shutdown, wake, and bounded-turn observations for one engine host.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::driver::ProducerDriverWake;

#[derive(Debug)]
pub(crate) struct EngineHostControl {
    shutdown: AtomicBool,
    #[cfg(test)]
    failure: AtomicBool,
    producer_turns: AtomicU64,
    driver_turns: AtomicU64,
    wake: ProducerDriverWake,
}

impl EngineHostControl {
    pub(super) const fn new(wake: ProducerDriverWake) -> Self {
        Self {
            shutdown: AtomicBool::new(false),
            #[cfg(test)]
            failure: AtomicBool::new(false),
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
    pub(super) fn request_failure(&self) {
        self.failure.store(true, Ordering::Release);
        let _wake_result = self.wake.request();
    }

    #[cfg(test)]
    pub(super) fn failure_requested(&self) -> bool {
        self.failure.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> EngineHostSnapshot {
        EngineHostSnapshot {
            producer_turns: self.producer_turns.load(Ordering::Relaxed),
            driver_turns: self.driver_turns.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct EngineHostSnapshot {
    pub(crate) producer_turns: u64,
    pub(crate) driver_turns: u64,
}
