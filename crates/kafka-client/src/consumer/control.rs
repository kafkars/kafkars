//! Thread-safe consumer control that does not mutate subscription state.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Thread-safe control operations that do not mutate subscription state.
#[derive(Debug, Clone, Default)]
pub struct ConsumerControl {
    wakeup_requested: Arc<AtomicBool>,
}

impl ConsumerControl {
    /// Interrupts a blocking receive or requests prompt task wakeup.
    pub fn wakeup(&self) {
        self.wakeup_requested.store(true, Ordering::Release);
    }

    /// Returns whether a wakeup has been requested in the prototype.
    pub fn wakeup_requested(&self) -> bool {
        self.wakeup_requested.load(Ordering::Acquire)
    }
}
