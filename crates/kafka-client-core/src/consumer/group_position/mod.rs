//! Assignment-fenced consumer-group position bootstrap policy.

mod effect;
mod input;
mod machine;
mod missing_offset;
mod model;
mod outcome;
mod reset;
mod transition;

pub use effect::{GroupPositionBootstrapEffect, GroupPositionBootstrapTransition};
pub use input::{
    GroupPositionBootstrapApplyError, GroupPositionBootstrapFetchFailure,
    GroupPositionBootstrapInput, GroupPositionBootstrapMachineError, GroupPositionBootstrapState,
};
pub use machine::GroupPositionBootstrapMachine;
pub use missing_offset::{GroupPositionMissingOffsetPolicy, GroupPositionMissingOffsetReset};
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
pub use reset::{
    GroupPositionResetApplyError, GroupPositionResetEffect, GroupPositionResetFailure,
    GroupPositionResetInput, GroupPositionResetMachine, GroupPositionResetMachineError,
    GroupPositionResetState, GroupPositionResetTerminal, GroupPositionResetTransition,
};

#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod missing_offset_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod rejection_test;
#[cfg(test)]
mod reset_test;
#[cfg(test)]
mod transition_test;
