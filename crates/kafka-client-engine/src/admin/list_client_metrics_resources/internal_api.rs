//! Crate-private ownership vocabulary for one client-metrics resource listing.

pub(crate) use super::{
    LIST_CLIENT_METRICS_RESOURCES_CAPACITY, ListClientMetricsResourcesAdmissionPort,
    ListClientMetricsResourcesHost, ListClientMetricsResourcesHostError,
    ListClientMetricsResourcesShardLockError, ListClientMetricsResourcesShardOwner,
    ListClientMetricsResourcesShardWake, ListClientMetricsResourcesShardWakeError,
    ListClientMetricsResourcesTurn,
};
