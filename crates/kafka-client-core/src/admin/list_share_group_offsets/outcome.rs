//! Declarative facade for bounded API-90 result and terminal facts.

mod batch;
mod error;
mod partition;
mod terminal;

pub use batch::{
    LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS,
    LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TOPICS,
    LIST_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES, ListShareGroupOffsetsBatch,
};
pub use error::{
    LIST_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES, ListShareGroupOffsetsBrokerError,
    ListShareGroupOffsetsFailure, ListShareGroupOffsetsFailureKind,
    ListShareGroupOffsetsPartitionBrokerError,
};
pub use partition::{
    ListShareGroupOffsetDescription, ListShareGroupOffsetOutcome, ListShareGroupOffsetResult,
};
pub use terminal::{
    ListShareGroupOffsetsBatchOutcome, ListShareGroupOffsetsTerminal, ListShareGroupsOffsetsBatch,
};
