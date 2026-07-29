//! Declarative facade for deterministic share-group offset alteration policy.

mod correlation;
mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    AlterShareGroupOffsetsEffect, AlterShareGroupOffsetsInput, AlterShareGroupOffsetsMachine,
    AlterShareGroupOffsetsMachineError, AlterShareGroupOffsetsState,
    AlterShareGroupOffsetsTransition,
};
pub use model::{
    ALTER_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES, ALTER_SHARE_GROUP_OFFSETS_MAX_PARTITIONS,
    ALTER_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES,
    ALTER_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES, AlterShareGroupOffset,
    AlterShareGroupOffsetsPlan, AlterShareGroupOffsetsPlanError,
};
pub use outcome::{
    ALTER_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES, ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS,
    ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES,
    ALTER_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES, AlterShareGroupOffsetsBatch,
    AlterShareGroupOffsetsBrokerError, AlterShareGroupOffsetsFailure,
    AlterShareGroupOffsetsFailureKind, AlterShareGroupOffsetsPartitionBrokerError,
    AlterShareGroupOffsetsPartitionOutcome, AlterShareGroupOffsetsPartitionResult,
    AlterShareGroupOffsetsTerminal,
};

#[cfg(test)]
mod correlation_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
