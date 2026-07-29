//! Declarative facade for deterministic share-group offset deletion policy.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    DeleteShareGroupOffsetsEffect, DeleteShareGroupOffsetsInput, DeleteShareGroupOffsetsMachine,
    DeleteShareGroupOffsetsMachineError, DeleteShareGroupOffsetsState,
    DeleteShareGroupOffsetsTransition,
};
pub use model::{
    DELETE_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES,
    DELETE_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES,
    DELETE_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES, DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS,
    DeleteShareGroupOffsetsPlan, DeleteShareGroupOffsetsPlanError,
};
pub use outcome::{
    DELETE_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES,
    DELETE_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES,
    DELETE_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES, DeleteShareGroupOffsetsBatch,
    DeleteShareGroupOffsetsBrokerError, DeleteShareGroupOffsetsFailure,
    DeleteShareGroupOffsetsFailureKind, DeleteShareGroupOffsetsTerminal,
    DeleteShareGroupOffsetsTopicBrokerError, DeleteShareGroupOffsetsTopicOutcome,
    DeleteShareGroupOffsetsTopicResult,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
