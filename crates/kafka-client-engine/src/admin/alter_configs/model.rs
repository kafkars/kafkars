//! Engine-owned resource strings and retained-capacity facts for incremental changes.

use kafka_client_core::{IncrementalAlterConfigsPlan, IncrementalAlterConfigsPlanError};

use crate::admin::retention::{
    RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, request_charge, result_fixed_charge,
};

mod resource;

pub use resource::{
    IncrementalConfigAlteration, IncrementalConfigOperation, IncrementalConfigResourceAlterations,
    TopicConfigAlterations,
};

/// One ordered incremental configuration request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigsRequest {
    resources: Vec<IncrementalConfigResourceAlterations>,
    validate_only: bool,
    topic_compatibility: bool,
}

impl IncrementalAlterConfigsRequest {
    /// Creates one ordered request.
    pub const fn new(topics: Vec<TopicConfigAlterations>) -> Self {
        Self {
            resources: topics,
            validate_only: false,
            topic_compatibility: true,
        }
    }

    /// Creates one ordered resource-generic request.
    pub const fn for_resources(resources: Vec<IncrementalConfigResourceAlterations>) -> Self {
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
                .map(IncrementalConfigResourceAlterations::canonicalize)
                .collect(),
        );
        self
    }

    pub(crate) fn retention(&self) -> Option<IncrementalAlterConfigsRetention> {
        let resource_count = self.resources.len();
        let alteration_count = self.resources.iter().try_fold(0usize, |count, resource| {
            count.checked_add(resource.alteration_count())
        })?;
        let text_bytes = self.resources.iter().try_fold(0usize, |bytes, resource| {
            bytes.checked_add(resource.text_bytes()?)
        })?;
        let request = request_charge(resource_count, alteration_count, text_bytes)?;
        let result_limit = result_limit_for_resources(
            resource_count,
            self.resources
                .iter()
                .map(IncrementalConfigResourceAlterations::resource_name_bytes),
        )?;
        Some(IncrementalAlterConfigsRetention {
            total: request,
            result_limit,
        })
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<IncrementalAlterConfigsPlan, IncrementalAlterConfigsPlanError> {
        let resources = self
            .resources
            .into_iter()
            .map(IncrementalConfigResourceAlterations::into_core)
            .collect();
        if self.topic_compatibility {
            IncrementalAlterConfigsPlan::new(resources, self.validate_only)
        } else {
            IncrementalAlterConfigsPlan::for_resources(resources, self.validate_only)
        }
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.resources.capacity() == self.resources.len()
            && self
                .resources
                .iter()
                .all(IncrementalConfigResourceAlterations::storage_is_canonical)
    }
}

pub(crate) fn incremental_alter_configs_result_limit(
    plan: &IncrementalAlterConfigsPlan,
) -> Option<usize> {
    result_limit_for_resources(
        plan.resources().len(),
        plan.resources()
            .iter()
            .map(|resource| resource.resource_name().len()),
    )
}

#[derive(Clone, Copy)]
pub(crate) struct IncrementalAlterConfigsRetention {
    total: usize,
    result_limit: usize,
}

impl IncrementalAlterConfigsRetention {
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

fn result_limit_for_resources(
    resource_count: usize,
    mut resource_name_bytes: impl Iterator<Item = usize>,
) -> Option<usize> {
    let resource_name_bytes = resource_name_bytes.try_fold(0usize, usize::checked_add)?;
    result_fixed_charge(resource_count, resource_name_bytes)?
        .checked_add(resource_count.checked_mul(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)?)
}
