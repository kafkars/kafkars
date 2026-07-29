//! Deterministic policy for one bounded API-74 v1 configuration-resource listing.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    ListConfigResourcesEffect, ListConfigResourcesInput, ListConfigResourcesMachine,
    ListConfigResourcesMachineError, ListConfigResourcesState, ListConfigResourcesTransition,
};
pub use model::{
    ConfigResourceType, ConfigResourceTypeError, LIST_CONFIG_RESOURCES_MAX_REQUEST_TYPES,
    ListConfigResourcesPlan, ListConfigResourcesPlanError,
};
pub use outcome::{
    LIST_CONFIG_RESOURCES_MAX_RESOURCE_NAME_BYTES, LIST_CONFIG_RESOURCES_MAX_RESOURCES,
    LIST_CONFIG_RESOURCES_MAX_TEXT_BYTES, ListConfigResourcesBrokerError,
    ListConfigResourcesFailure, ListConfigResourcesFailureKind, ListConfigResourcesListing,
    ListConfigResourcesTerminal, ListedConfigResource,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
