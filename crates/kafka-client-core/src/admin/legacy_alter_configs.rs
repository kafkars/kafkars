//! Declarative facade for deterministic resource-generic `LegacyAlterConfigs` policy.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    LegacyAlterConfigsEffect, LegacyAlterConfigsInput, LegacyAlterConfigsMachine,
    LegacyAlterConfigsMachineError, LegacyAlterConfigsState, LegacyAlterConfigsTransition,
};
pub use model::{
    LegacyAlterConfigsPlan, LegacyAlterConfigsPlanError, LegacyConfigEntry,
    LegacyConfigResourceReplacement, LegacyTopicConfigReplacement,
};
pub use outcome::{
    LegacyAlterConfigBrokerError, LegacyAlterConfigOutcome, LegacyAlterConfigResult,
    LegacyAlterConfigsBatch, LegacyAlterConfigsFailure, LegacyAlterConfigsFailureKind,
    LegacyAlterConfigsTerminal,
};

#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
