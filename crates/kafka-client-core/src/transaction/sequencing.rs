//! Declarative facade for epoch-fenced transactional partition sequencing.

mod machine;
mod model;
mod preflight;

pub use machine::TransactionSequenceMachine;
pub use model::{
    TransactionPartition, TransactionSequenceMachineError, TransactionSequenceSettlement,
    TransactionSequenceState,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
