//! Declarative facade for deterministic topic `IncrementalAlterConfigs` policy.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    IncrementalAlterConfigsEffect, IncrementalAlterConfigsInput, IncrementalAlterConfigsMachine,
    IncrementalAlterConfigsMachineError, IncrementalAlterConfigsState,
    IncrementalAlterConfigsTransition,
};
pub use model::{
    ConfigAlteration, ConfigAlterationOperation, IncrementalAlterConfigsPlan,
    IncrementalAlterConfigsPlanError, TopicConfigAlteration,
};
pub use outcome::{
    IncrementalAlterConfigBrokerError, IncrementalAlterConfigOutcome, IncrementalAlterConfigResult,
    IncrementalAlterConfigsBatch, IncrementalAlterConfigsFailure,
    IncrementalAlterConfigsFailureKind, IncrementalAlterConfigsTerminal,
};

#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
