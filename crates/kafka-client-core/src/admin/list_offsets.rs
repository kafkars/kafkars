//! Declarative facade for deterministic Admin `ListOffsets` policy.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    AdminListOffsetsEffect, AdminListOffsetsInput, AdminListOffsetsMachine,
    AdminListOffsetsMachineError, AdminListOffsetsState, AdminListOffsetsTransition,
};
pub use model::{
    AdminListOffsetSpec, AdminListOffsetTarget, AdminListOffsetsPlan, AdminListOffsetsPlanError,
};
pub use outcome::{
    AdminListOffset, AdminListOffsetBrokerError, AdminListOffsetOutcome, AdminListOffsetResult,
    AdminListOffsetsBatch, AdminListOffsetsFailure, AdminListOffsetsFailureKind,
    AdminListOffsetsTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
