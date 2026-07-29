//! Generated-free normalized facts from one configuration-resource listing.

/// One opaque positive Kafka configuration-resource type and its UTF-8 name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListConfigResource {
    resource_type: i8,
    resource_name: String,
}

impl ListConfigResource {
    pub(super) const fn new(resource_type: i8, resource_name: String) -> Self {
        Self {
            resource_type,
            resource_name,
        }
    }

    pub(crate) fn into_parts(self) -> (i8, String) {
        (self.resource_type, self.resource_name)
    }

    pub(super) const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    pub(super) fn resource_name(&self) -> &str {
        &self.resource_name
    }

    pub(super) const fn resource_name_capacity(&self) -> usize {
        self.resource_name.capacity()
    }
}

/// Exact top-level broker code and canonical resource facts from API key 74.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListConfigResourcesResponseFacts {
    throttle_time_ms: u32,
    broker_error_code: i16,
    resources: Vec<ListConfigResource>,
    retained_bytes: usize,
}

impl ListConfigResourcesResponseFacts {
    pub(super) const fn new(
        throttle_time_ms: u32,
        broker_error_code: i16,
        resources: Vec<ListConfigResource>,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            broker_error_code,
            resources,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (u32, i16, Vec<ListConfigResource>, usize) {
        (
            self.throttle_time_ms,
            self.broker_error_code,
            self.resources,
            self.retained_bytes,
        )
    }

    #[cfg(test)]
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    #[cfg(test)]
    pub(crate) const fn broker_error_code(&self) -> i16 {
        self.broker_error_code
    }

    #[cfg(test)]
    pub(crate) fn resources(&self) -> &[ListConfigResource] {
        &self.resources
    }

    #[cfg(test)]
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}
