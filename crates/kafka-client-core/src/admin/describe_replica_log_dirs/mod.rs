//! Deterministic policy for caller-selected replica log-directory descriptions.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    DescribeReplicaLogDirsEffect, DescribeReplicaLogDirsInput, DescribeReplicaLogDirsMachine,
    DescribeReplicaLogDirsMachineError, DescribeReplicaLogDirsState,
    DescribeReplicaLogDirsTransition,
};
pub use model::{
    DESCRIBE_REPLICA_LOG_DIRS_MAX_TOPIC_BYTES, DescribeReplicaLogDirsPlan,
    DescribeReplicaLogDirsPlanError, DescribeReplicaLogDirsReplica,
};
pub use outcome::{
    DescribeReplicaLogDirsBatch, DescribeReplicaLogDirsBrokerError, DescribeReplicaLogDirsFailure,
    DescribeReplicaLogDirsFailureKind, DescribeReplicaLogDirsReplicaOutcome,
    DescribeReplicaLogDirsReplicaPlacement, DescribeReplicaLogDirsReplicaResult,
    DescribeReplicaLogDirsTerminal, ReplicaLogDirInfo, ReplicaLogDirLocation,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
