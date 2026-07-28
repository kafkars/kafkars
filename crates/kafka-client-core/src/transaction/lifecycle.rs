//! Declarative facade for one deterministic transactional-producer lifecycle.

mod effect;
mod ending;
mod input;
mod machine;
mod model;
mod state;
mod transition;

pub use effect::{TransactionLifecycleEffect, TransactionLifecycleTransition};
pub use input::TransactionLifecycleInput;
pub use machine::{TransactionLifecycleMachine, TransactionLifecycleMachineError};
pub use model::{
    TransactionEndMode, TransactionEndObservation, TransactionEndOutcome, TransactionEpoch,
    TransactionLifecycleTerminal,
};
pub use state::TransactionLifecycleState;

#[cfg(test)]
mod ending_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod transition_test;
