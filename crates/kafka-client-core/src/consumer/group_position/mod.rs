//! Assignment-fenced consumer-group position bootstrap policy.

mod effect;
mod input;
mod machine;
mod model;
mod outcome;
mod transition;

pub use effect::{GroupPositionBootstrapEffect, GroupPositionBootstrapTransition};
pub use input::{
    GroupPositionBootstrapApplyError, GroupPositionBootstrapFetchFailure,
    GroupPositionBootstrapInput, GroupPositionBootstrapMachineError, GroupPositionBootstrapState,
};
pub use machine::GroupPositionBootstrapMachine;
pub use model::{
    GroupPositionBatch, GroupPositionBootstrapBuildError, GroupPositionBootstrapBuildErrorKind,
    GroupPositionBrokerError, GroupPositionFence, GroupPositionPartitionFact,
    GroupPositionPartitionResult,
};
pub use outcome::{
    GroupPositionBootstrapFailure, GroupPositionBootstrapFailureKind,
    GroupPositionBootstrapMissingOffsets, GroupPositionBootstrapPartitionRejection,
    GroupPositionBootstrapTerminal,
};

#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod rejection_test;
#[cfg(test)]
mod transition_test;
