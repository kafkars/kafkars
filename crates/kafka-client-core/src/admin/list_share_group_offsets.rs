//! Declarative facade for deterministic share-group offset listing policy.

mod correlation;
mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    ListShareGroupOffsetsEffect, ListShareGroupOffsetsInput, ListShareGroupOffsetsMachine,
    ListShareGroupOffsetsMachineError, ListShareGroupOffsetsState, ListShareGroupOffsetsTransition,
};
pub(crate) use model::ListShareGroupOffsetsPlanShape;
pub use model::{
    LIST_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_GROUPS,
    LIST_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES,
    LIST_SHARE_GROUP_OFFSETS_MAX_SELECTED_PARTITIONS,
    LIST_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES, ListShareGroupOffsetTarget,
    ListShareGroupOffsetsPlan, ListShareGroupOffsetsPlanError, ListShareGroupOffsetsQuery,
    ListShareGroupOffsetsSelection,
};
pub use outcome::{
    LIST_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS,
    LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TOPICS,
    LIST_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES, ListShareGroupOffsetDescription,
    ListShareGroupOffsetOutcome, ListShareGroupOffsetResult, ListShareGroupOffsetsBatch,
    ListShareGroupOffsetsBatchOutcome, ListShareGroupOffsetsBrokerError,
    ListShareGroupOffsetsFailure, ListShareGroupOffsetsFailureKind,
    ListShareGroupOffsetsPartitionBrokerError, ListShareGroupOffsetsTerminal,
    ListShareGroupsOffsetsBatch,
};

#[cfg(test)]
mod batch_test;
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
