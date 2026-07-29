//! Engine-owned resource strings and retained-capacity facts for API 33 snapshots.

use kafka_client_core::{LegacyAlterConfigsPlan, LegacyAlterConfigsPlanError};

use crate::admin::retention::{
    RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, request_charge, result_fixed_charge,
};

mod resource;

pub use resource::{
    LegacyConfigEntry, LegacyConfigResourceReplacement, LegacyTopicConfigReplacement,
};

/// One ordered legacy full-snapshot request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyAlterConfigsRequest {
    resources: Vec<LegacyConfigResourceReplacement>,
    validate_only: bool,
    topic_compatibility: bool,
}

impl LegacyAlterConfigsRequest {
    /// Creates one ordered request.
    pub const fn new(topics: Vec<LegacyTopicConfigReplacement>) -> Self {
        Self {
            resources: topics,
            validate_only: false,
            topic_compatibility: true,
        }
    }

    /// Creates one ordered resource-generic request.
    pub const fn for_resources(resources: Vec<LegacyConfigResourceReplacement>) -> Self {
        Self {
            resources,
            validate_only: false,
            topic_compatibility: false,
        }
    }

    /// Selects broker validation without mutation.
    #[must_use]
    pub const fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.resources = canonical_vec(
            self.resources
                .into_iter()
                .map(LegacyConfigResourceReplacement::canonicalize)
                .collect(),
        );
        self
    }

    pub(crate) fn retention(&self) -> Option<LegacyAlterConfigsRetention> {
        let resource_count = self.resources.len();
        let config_count = self.resources.iter().try_fold(0usize, |count, resource| {
            count.checked_add(resource.config_count())
        })?;
        let text_bytes = self.resources.iter().try_fold(0usize, |bytes, resource| {
            bytes.checked_add(resource.text_bytes()?)
        })?;
        let resource_name_bytes = self.resources.iter().try_fold(0usize, |bytes, resource| {
            bytes.checked_add(resource.resource_name_bytes())
        })?;
        let request = request_charge(resource_count, config_count, text_bytes)?;
        let result_limit = result_fixed_charge(resource_count, resource_name_bytes)?
            .checked_add(resource_count.checked_mul(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)?)?;
        Some(LegacyAlterConfigsRetention {
            total: request,
            result_limit,
        })
    }

    pub(crate) fn into_plan(self) -> Result<LegacyAlterConfigsPlan, LegacyAlterConfigsPlanError> {
        let resources = self
            .resources
            .into_iter()
            .map(LegacyConfigResourceReplacement::into_core)
            .collect();
        if self.topic_compatibility {
            LegacyAlterConfigsPlan::new(resources, self.validate_only)
        } else {
            LegacyAlterConfigsPlan::for_resources(resources, self.validate_only)
        }
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.resources.capacity() == self.resources.len()
            && self
                .resources
                .iter()
                .all(LegacyConfigResourceReplacement::storage_is_canonical)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct LegacyAlterConfigsRetention {
    total: usize,
    result_limit: usize,
}

impl LegacyAlterConfigsRetention {
    pub(crate) const fn total(self) -> usize {
        self.total
    }

    pub(crate) const fn result_limit(self) -> usize {
        self.result_limit
    }

    #[cfg(test)]
    pub(crate) const fn from_parts(total: usize, result_limit: usize) -> Self {
        Self {
            total,
            result_limit,
        }
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
