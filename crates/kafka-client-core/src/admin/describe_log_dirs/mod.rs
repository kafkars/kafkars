//! Deterministic policy for caller-selected broker log-directory description.

mod failure;
mod machine;
mod model;
mod outcome;
mod transition;
mod value;

pub use machine::{
    AdminDescribeLogDirsEffect, AdminDescribeLogDirsInput, AdminDescribeLogDirsMachine,
    AdminDescribeLogDirsMachineError, AdminDescribeLogDirsState, AdminDescribeLogDirsTransition,
};
pub use model::{AdminDescribeLogDirsPlan, AdminDescribeLogDirsPlanError};
pub use outcome::{
    AdminDescribeLogDirsBatch, AdminDescribeLogDirsBrokerOutcome, AdminDescribeLogDirsBrokerResult,
    AdminDescribeLogDirsFailure, AdminDescribeLogDirsFailureKind, AdminDescribeLogDirsTerminal,
};
pub use value::{
    AdminDescribeLogDirsBrokerError, AdminLogDirDescription, AdminLogDirOutcome,
    AdminLogDirReplicaInfo, AdminLogDirResult,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
