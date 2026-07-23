//! Virtual time and deterministic effect execution for client semantics.

#![forbid(unsafe_code)]

mod clock;
mod error;
mod producer;
mod state;

pub use clock::{VirtualClock, VirtualClockError};
pub use error::SimulationError;
pub use producer::ProducerScenario;

#[cfg(test)]
mod clock_test;
#[cfg(test)]
mod producer_test;
#[cfg(test)]
mod producer_timer_test;
