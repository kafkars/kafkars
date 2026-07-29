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
pub use model::{
    ADMIN_DESCRIBE_LOG_DIRS_MAX_PARTITIONS, ADMIN_DESCRIBE_LOG_DIRS_MAX_TOPIC_BYTES,
    ADMIN_DESCRIBE_LOG_DIRS_MAX_TOPICS, AdminDescribeLogDirsPartition, AdminDescribeLogDirsPlan,
    AdminDescribeLogDirsPlanError, AdminDescribeLogDirsSelection,
};
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
