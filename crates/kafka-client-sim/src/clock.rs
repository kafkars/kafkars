//! Monotonic virtual time for deterministic scenarios.

use kafka_client_core::Moment;

/// Virtual time cannot represent the requested monotonic target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VirtualClockError;

impl core::fmt::Display for VirtualClockError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("virtual monotonic time overflow")
    }
}

impl std::error::Error for VirtualClockError {}

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

    /// Advances the clock by a checked deterministic number of ticks.
    pub const fn advance(&mut self, ticks: u64) -> Result<(), VirtualClockError> {
        match self.target_after(ticks) {
            Ok(target) => {
                self.set(target);
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) const fn target_after(self, ticks: u64) -> Result<Moment, VirtualClockError> {
        match self.tick.checked_add(ticks) {
            Some(target) => Ok(Moment::from_tick(target)),
            None => Err(VirtualClockError),
        }
    }

    pub(crate) const fn set(&mut self, moment: Moment) {
        self.tick = moment.tick();
    }
}
