//! Deterministic policy for caller-ordered replica log-directory alterations.

mod failure;
mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    AlterReplicaLogDirsEffect, AlterReplicaLogDirsInput, AlterReplicaLogDirsMachine,
    AlterReplicaLogDirsMachineError, AlterReplicaLogDirsState, AlterReplicaLogDirsTransition,
};
pub use model::{
    AlterReplicaLogDirAssignment, AlterReplicaLogDirsPlan, AlterReplicaLogDirsPlanError,
};
pub use outcome::{
    AlterReplicaLogDirBrokerError, AlterReplicaLogDirOutcome, AlterReplicaLogDirResult,
    AlterReplicaLogDirsBatch, AlterReplicaLogDirsFailure, AlterReplicaLogDirsFailureKind,
    AlterReplicaLogDirsTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
