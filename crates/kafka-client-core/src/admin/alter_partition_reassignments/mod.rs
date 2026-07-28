//! Deterministic ownership for one partition-reassignment alteration.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    AlterPartitionReassignmentsEffect, AlterPartitionReassignmentsInput,
    AlterPartitionReassignmentsMachine, AlterPartitionReassignmentsMachineError,
    AlterPartitionReassignmentsState, AlterPartitionReassignmentsTransition,
};
pub use model::{
    AlterPartitionReassignment, AlterPartitionReassignmentsPlan,
    AlterPartitionReassignmentsPlanError, PartitionReassignmentTarget,
};
pub use outcome::{
    AlterPartitionReassignmentBrokerError, AlterPartitionReassignmentOutcome,
    AlterPartitionReassignmentResult, AlterPartitionReassignmentsBatch,
    AlterPartitionReassignmentsFailure, AlterPartitionReassignmentsFailureKind,
    AlterPartitionReassignmentsTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
