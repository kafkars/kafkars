//! Declarative facade for singular and batched group-offset terminals.

mod batch;
mod failure;
mod offset;

pub use batch::{
    ListConsumerGroupBatchOutcome, ListConsumerGroupOffsetsBatch, ListConsumerGroupsOffsetsBatch,
};
pub use failure::{
    ListConsumerGroupOffsetsFailure, ListConsumerGroupOffsetsFailureKind,
    ListConsumerGroupOffsetsTerminal,
};
pub use offset::{
    GroupOffsetBrokerError, GroupOffsetDescription, GroupOffsetOutcome, GroupOffsetResult,
};
