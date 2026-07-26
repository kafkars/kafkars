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
pub use model::{ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsPlanError};
pub use outcome::{
    GroupOffsetBrokerError, GroupOffsetDescription, GroupOffsetOutcome, GroupOffsetResult,
    ListConsumerGroupOffsetsBatch, ListConsumerGroupOffsetsFailure,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
