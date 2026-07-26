//! Declarative facade for deterministic consumer-group offset alteration policy.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    AlterConsumerGroupOffsetsEffect, AlterConsumerGroupOffsetsInput,
    AlterConsumerGroupOffsetsMachine, AlterConsumerGroupOffsetsMachineError,
    AlterConsumerGroupOffsetsState, AlterConsumerGroupOffsetsTransition,
};
pub use model::{
    AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsPlan,
    AlterConsumerGroupOffsetsPlanError,
};
pub use outcome::{
    AlterConsumerGroupOffsetBrokerError, AlterConsumerGroupOffsetOutcome,
    AlterConsumerGroupOffsetResult, AlterConsumerGroupOffsetsBatch,
    AlterConsumerGroupOffsetsFailure, AlterConsumerGroupOffsetsFailureKind,
    AlterConsumerGroupOffsetsTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
