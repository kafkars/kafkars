//! Producer-operation ownership from admission through terminal settlement.

mod cancellation;
#[cfg(test)]
mod cancellation_test;
mod state;
mod transition;

pub use state::{ProducerOperation, ProducerOperationState};
