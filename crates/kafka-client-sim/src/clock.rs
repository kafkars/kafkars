//! Monotonic virtual time for deterministic scenarios.

use kafka_client_core::Moment;

/// Monotonic virtual clock used by deterministic scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VirtualClock {
    tick: u64,
}

impl VirtualClock {
    /// Returns the current absolute tick.
    pub const fn now(self) -> Moment {
        Moment::from_tick(self.tick)
    }

    /// Advances the clock by a deterministic number of ticks.
    pub const fn advance(&mut self, ticks: u64) {
        self.tick = self.tick.saturating_add(ticks);
    }
}
