//! Inert configuration-resource listing intent with one submission boundary.

use std::time::Duration;

use crate::bridge::admin::AdminEngine;

use super::{ConfigResourceType, ListConfigResources};

/// Inert request to list Kafka configuration-resource identities.
#[must_use = "call submit to admit the ListConfigResources operation"]
pub struct ListConfigResourcesBuilder {
    engine: AdminEngine,
    resource_types: Vec<ConfigResourceType>,
    timeout: Duration,
}

impl ListConfigResourcesBuilder {
    pub(crate) const fn new(engine: AdminEngine, timeout: Duration) -> Self {
        Self {
            engine,
            resource_types: Vec::new(),
            timeout,
        }
    }

    /// Replaces the requested resource-type filter.
    ///
    /// An empty filter asks Kafka for every supported configuration-resource
    /// type. Validation of raw type codes remains deferred to [`Self::submit`].
    pub fn resource_types(mut self, resource_types: Vec<ConfigResourceType>) -> Self {
        self.resource_types = resource_types;
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> ListConfigResources {
        ListConfigResources::from_bridge(
            self.engine
                .submit_list_config_resources(self.resource_types, self.timeout),
        )
    }
}

impl std::fmt::Debug for ListConfigResourcesBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListConfigResourcesBuilder")
            .field("resource_types", &self.resource_types)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
