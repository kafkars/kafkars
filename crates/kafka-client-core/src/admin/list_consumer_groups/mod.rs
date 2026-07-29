//! Deterministic cluster-wide consumer-group discovery and listing policy.

mod machine;
mod outcome;
mod plan;
mod transition;

pub use machine::{
    AdminGroupListingScope, AdminListConsumerGroupsEffect, AdminListConsumerGroupsInput,
    AdminListConsumerGroupsMachine, AdminListConsumerGroupsMachineError,
    AdminListConsumerGroupsState, AdminListConsumerGroupsTransition,
};
pub use outcome::{
    AdminConsumerGroupListing, AdminListConsumerGroupsBatch, AdminListConsumerGroupsBrokerError,
    AdminListConsumerGroupsBrokerOutcome, AdminListConsumerGroupsFailure,
    AdminListConsumerGroupsFailureKind, AdminListConsumerGroupsTerminal,
};
pub use plan::{AdminGroupListingFilters, AdminGroupListingFiltersError};

#[cfg(test)]
mod plan_test;
#[cfg(test)]
mod transition_test;
