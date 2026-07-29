//! Declarative facade for deterministic one-group offset listing policy.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    ListConsumerGroupOffsetsEffect, ListConsumerGroupOffsetsInput, ListConsumerGroupOffsetsMachine,
    ListConsumerGroupOffsetsMachineError, ListConsumerGroupOffsetsState,
    ListConsumerGroupOffsetsTransition,
};
pub use model::{
    ListConsumerGroupOffsetTarget, ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsPlanError,
    ListConsumerGroupOffsetsQuery, ListConsumerGroupOffsetsSelection,
};
pub use outcome::{
    GroupOffsetBrokerError, GroupOffsetDescription, GroupOffsetOutcome, GroupOffsetResult,
    ListConsumerGroupBatchOutcome, ListConsumerGroupOffsetsBatch, ListConsumerGroupOffsetsFailure,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsTerminal,
    ListConsumerGroupsOffsetsBatch,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
