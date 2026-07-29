//! Deterministic policy for one fixed API-74 client-metrics resource listing.

mod machine;
mod outcome;
mod transition;

pub use machine::{
    ListClientMetricsResourcesEffect, ListClientMetricsResourcesInput,
    ListClientMetricsResourcesMachine, ListClientMetricsResourcesMachineError,
    ListClientMetricsResourcesState, ListClientMetricsResourcesTransition,
};
pub use outcome::{
    LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCE_NAME_BYTES,
    LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCES, LIST_CLIENT_METRICS_RESOURCES_MAX_RETAINED_BYTES,
    ListClientMetricsResourcesBrokerError, ListClientMetricsResourcesFailure,
    ListClientMetricsResourcesFailureKind, ListClientMetricsResourcesListing,
    ListClientMetricsResourcesTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
