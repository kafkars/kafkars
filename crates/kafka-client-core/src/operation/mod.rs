//! Producer-operation ownership from admission through terminal settlement.

mod state;
mod transition;

pub use state::{ProducerOperation, ProducerOperationState};
