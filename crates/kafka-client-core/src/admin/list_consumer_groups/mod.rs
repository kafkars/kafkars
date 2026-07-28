//! Deterministic cluster-wide consumer-group discovery and listing policy.

mod machine;
mod outcome;
mod transition;

pub use machine::{
    AdminListConsumerGroupsEffect, AdminListConsumerGroupsInput, AdminListConsumerGroupsMachine,
    AdminListConsumerGroupsMachineError, AdminListConsumerGroupsState,
    AdminListConsumerGroupsTransition,
};
pub use outcome::{
    AdminConsumerGroupListing, AdminListConsumerGroupsBatch, AdminListConsumerGroupsBrokerError,
    AdminListConsumerGroupsBrokerOutcome, AdminListConsumerGroupsFailure,
    AdminListConsumerGroupsFailureKind, AdminListConsumerGroupsTerminal,
};

#[cfg(test)]
mod transition_test;
