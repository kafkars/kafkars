//! Deterministic policy for concrete Kafka admin operations.

mod machine;
mod model;
mod outcome;
mod transition;

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

#[cfg(test)]
mod model_test;
#[cfg(test)]
mod transition_test;
