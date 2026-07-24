//! Deterministic policy for concrete Kafka admin operations.

mod delete_machine;
mod delete_model;
mod delete_outcome;
mod delete_transition;
mod describe_machine;
mod describe_outcome;
mod describe_transition;
mod machine;
mod model;
mod outcome;
mod partitions_machine;
mod partitions_model;
mod partitions_outcome;
mod partitions_transition;
mod transition;

pub use delete_machine::{
    DeleteTopicsEffect, DeleteTopicsInput, DeleteTopicsMachine, DeleteTopicsMachineError,
    DeleteTopicsState, DeleteTopicsTransition,
};
pub use delete_model::{DeleteTopicsPlan, DeleteTopicsPlanError};
pub use delete_outcome::{
    DeleteTopicBrokerError, DeleteTopicOutcome, DeleteTopicResult, DeleteTopicsFailure,
    DeleteTopicsFailureKind, DeleteTopicsTerminal,
};
pub use describe_machine::{
    DescribeClusterEffect, DescribeClusterInput, DescribeClusterMachine,
    DescribeClusterMachineError, DescribeClusterState, DescribeClusterTransition,
};
pub use describe_outcome::{
    ClusterBroker, ClusterDescription, DescribeClusterBrokerError, DescribeClusterFailure,
    DescribeClusterFailureKind, DescribeClusterTerminal,
};
pub use machine::{
    CreateTopicsEffect, CreateTopicsInput, CreateTopicsMachine, CreateTopicsMachineError,
    CreateTopicsState, CreateTopicsTransition,
};
pub use model::{
    CreateTopicConfig, CreateTopicSpecification, CreateTopicsPlan, CreateTopicsPlanError,
};
pub use outcome::{
    CreateTopicBrokerError, CreateTopicOutcome, CreateTopicResult, CreateTopicsFailure,
    CreateTopicsFailureKind, CreateTopicsTerminal,
};
pub use partitions_machine::{
    CreatePartitionsEffect, CreatePartitionsInput, CreatePartitionsMachine,
    CreatePartitionsMachineError, CreatePartitionsState, CreatePartitionsTransition,
};
pub use partitions_model::{
    CreatePartitionsPlan, CreatePartitionsPlanError, CreatePartitionsSpecification,
};
pub use partitions_outcome::{
    CreatePartitionsFailure, CreatePartitionsFailureKind, CreatePartitionsTerminal,
    PartitionIncreaseBrokerError, PartitionIncreaseOutcome, PartitionIncreaseResult,
};

#[cfg(test)]
mod delete_model_test;
#[cfg(test)]
mod delete_transition_test;
#[cfg(test)]
mod describe_transition_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod partitions_model_test;
#[cfg(test)]
mod partitions_transition_test;
#[cfg(test)]
mod transition_test;
