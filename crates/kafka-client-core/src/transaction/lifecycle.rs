//! Declarative facade for one deterministic transactional-producer lifecycle.

mod effect;
mod ending;
mod input;
mod machine;
mod model;
mod send_transition;
mod state;
mod transition;

pub use effect::{TransactionLifecycleEffect, TransactionLifecycleTransition};
pub use input::TransactionLifecycleInput;
pub use machine::{TransactionLifecycleMachine, TransactionLifecycleMachineError};
pub use model::{
    TransactionEndMode, TransactionEndObservation, TransactionEndOutcome, TransactionEpoch,
    TransactionLifecycleTerminal, TransactionSendAttempt, TransactionSendAttemptFailure,
    TransactionSendId, TransactionSendIdentity, TransactionSendOutcome, TransactionSequenceLease,
};
pub use state::TransactionLifecycleState;

#[cfg(test)]
mod ending_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod send_retry_safety_test;
#[cfg(test)]
mod send_retry_test_support;
#[cfg(test)]
mod send_retry_transition_test;
#[cfg(test)]
mod send_transition_test;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod transition_test;
