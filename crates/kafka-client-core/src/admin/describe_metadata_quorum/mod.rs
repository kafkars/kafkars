//! Deterministic policy for one fixed Admin `DescribeMetadataQuorum` query.

mod description;
mod machine;
mod node;
mod outcome;
mod replica;
mod transition;
mod validation;
mod value_error;

pub use description::{
    DESCRIBE_METADATA_QUORUM_MAX_LISTENERS_PER_NODE, DESCRIBE_METADATA_QUORUM_MAX_NODES,
    DESCRIBE_METADATA_QUORUM_MAX_REPLICAS, DescribeMetadataQuorumDescription,
};
pub use machine::{
    DescribeMetadataQuorumEffect, DescribeMetadataQuorumInput, DescribeMetadataQuorumMachine,
    DescribeMetadataQuorumMachineError, DescribeMetadataQuorumState,
    DescribeMetadataQuorumTransition,
};
pub use node::{DescribeMetadataQuorumListener, DescribeMetadataQuorumNode};
pub use outcome::{
    DESCRIBE_METADATA_QUORUM_DIAGNOSTIC_BYTES, DescribeMetadataQuorumBrokerError,
    DescribeMetadataQuorumFailure, DescribeMetadataQuorumFailureKind,
    DescribeMetadataQuorumPartitionError, DescribeMetadataQuorumTerminal,
};
pub use replica::DescribeMetadataQuorumReplica;
pub use value_error::DescribeMetadataQuorumValueError;

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
#[cfg(test)]
mod value_test;
