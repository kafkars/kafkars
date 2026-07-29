//! Generated-free bounded facts from one client-metrics resource listing.

/// Exact top-level error and canonical resource names from API-key 74.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListClientMetricsResourcesResponseFacts {
    throttle_time_ms: u32,
    broker_error_code: i16,
    resource_names: Vec<String>,
    retained_bytes: usize,
}

impl ListClientMetricsResourcesResponseFacts {
    pub(super) const fn new(
        throttle_time_ms: u32,
        broker_error_code: i16,
        resource_names: Vec<String>,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            broker_error_code,
            resource_names,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (u32, i16, Vec<String>, usize) {
        (
            self.throttle_time_ms,
            self.broker_error_code,
            self.resource_names,
            self.retained_bytes,
        )
    }
}
