//! Declarative facade for deterministic consumer-group offset deletion policy.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    DeleteConsumerGroupOffsetsEffect, DeleteConsumerGroupOffsetsInput,
    DeleteConsumerGroupOffsetsMachine, DeleteConsumerGroupOffsetsMachineError,
    DeleteConsumerGroupOffsetsState, DeleteConsumerGroupOffsetsTransition,
};
pub use model::{
    DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsPlan,
    DeleteConsumerGroupOffsetsPlanError,
};
pub use outcome::{
    DeleteConsumerGroupOffsetBrokerError, DeleteConsumerGroupOffsetOutcome,
    DeleteConsumerGroupOffsetResult, DeleteConsumerGroupOffsetsBatch,
    DeleteConsumerGroupOffsetsFailure, DeleteConsumerGroupOffsetsFailureKind,
    DeleteConsumerGroupOffsetsTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
