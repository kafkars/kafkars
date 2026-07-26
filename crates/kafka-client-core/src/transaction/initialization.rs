//! Declarative facade for one transactional-producer initialization.

mod effect;
mod input;
mod machine;
mod model;
mod outcome;
mod state;
mod transition;

pub use effect::{TransactionInitializationEffect, TransactionInitializationTransition};
pub use input::TransactionInitializationInput;
pub use machine::{TransactionInitializationMachine, TransactionInitializationMachineError};
pub use model::{
    TransactionInitializationPlan, TransactionInitializationPlanError, TransactionalOwnerId,
    TransactionalProducerIdentity,
};
pub use outcome::{
    TransactionInitializationBrokerCategory, TransactionInitializationBrokerFailure,
    TransactionInitializationFailure, TransactionInitializationFailureKind,
    TransactionInitializationTerminal,
};
pub use state::TransactionInitializationState;

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
