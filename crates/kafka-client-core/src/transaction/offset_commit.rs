//! Declarative facade for deterministic transactional offset transfer.

mod effect;
mod input;
mod machine;
mod model;
mod transition;

pub use effect::{TransactionOffsetCommitEffect, TransactionOffsetCommitTransition};
pub use input::TransactionOffsetCommitInput;
pub use machine::{TransactionOffsetCommitMachine, TransactionOffsetCommitMachineError};
pub use model::{
    TransactionOffsetCommitConsequence, TransactionOffsetCommitEndBarrier,
    TransactionOffsetCommitId, TransactionOffsetCommitStage, TransactionOffsetCommitState,
    TransactionOffsetCommitTerminal,
};

#[cfg(test)]
mod correlation_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod rejection_test;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod transition_retry_test;
#[cfg(test)]
mod transition_test;
