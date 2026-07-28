//! Declarative facade for deterministic Admin `DeleteConsumerGroups` policy.

mod broker_error;
mod machine;
mod model;
mod outcome;
mod transition;

pub use broker_error::{DELETE_CONSUMER_GROUPS_DIAGNOSTIC_BYTES, DeleteConsumerGroupsBrokerError};
pub use machine::{
    DeleteConsumerGroupsEffect, DeleteConsumerGroupsInput, DeleteConsumerGroupsMachine,
    DeleteConsumerGroupsMachineError, DeleteConsumerGroupsState, DeleteConsumerGroupsTransition,
};
pub use model::{
    DeleteConsumerGroupsPlan, DeleteConsumerGroupsPlanError, DeleteConsumerGroupsTarget,
};
pub use outcome::{
    DeleteConsumerGroupsBatch, DeleteConsumerGroupsFailure, DeleteConsumerGroupsFailureKind,
    DeleteConsumerGroupsOutcome, DeleteConsumerGroupsResult, DeleteConsumerGroupsTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
