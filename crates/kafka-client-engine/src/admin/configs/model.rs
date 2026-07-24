//! Engine-owned request values for bounded batches of topic `DescribeConfigs`.

use kafka_client_core::{
    DescribeConfigsPlan, DescribeConfigsPlanError,
    DescribeConfigsResourceQuery as CoreResourceQuery,
};

const TOPIC_RESOURCE_TYPE: i8 = 2;
const RESULT_BYTES_PER_RESOURCE: usize = 256 * 1024;

#[derive(Clone, Copy)]
pub(crate) struct DescribeConfigsRetention {
    total: usize,
    result_limit: usize,
}

impl DescribeConfigsRetention {
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

/// One ordered configuration-resource query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeConfigsResourceQuery {
    resource_type: i8,
    resource_name: String,
    configuration_keys: Option<Vec<String>>,
}

impl DescribeConfigsResourceQuery {
    /// Creates one raw resource query for validation at admission.
    pub const fn new(
        resource_type: i8,
        resource_name: String,
        configuration_keys: Option<Vec<String>>,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            configuration_keys,
        }
    }

    fn canonicalize(mut self) -> Self {
        self.resource_name = canonical_string(self.resource_name);
        if let Some(keys) = self.configuration_keys.as_mut() {
            for key in keys.iter_mut() {
                *key = canonical_string(std::mem::take(key));
            }
            *keys = std::mem::take(keys).into_boxed_slice().into_vec();
        }
        self
    }

    fn text_bytes(&self) -> Option<usize> {
        self.configuration_keys
            .as_deref()
            .unwrap_or_default()
            .iter()
            .try_fold(self.resource_name.len(), |bytes, key| {
                bytes.checked_add(key.len())
            })
    }

    fn into_core(self) -> CoreResourceQuery {
        CoreResourceQuery::new(
            self.resource_type,
            self.resource_name,
            self.configuration_keys,
        )
    }
}

/// One ordered batch-native `DescribeConfigs` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeConfigsRequest {
    resources: Vec<DescribeConfigsResourceQuery>,
    include_synonyms: bool,
    include_documentation: bool,
}

impl DescribeConfigsRequest {
    /// Creates one raw batch for deterministic admission validation.
    pub const fn new(
        resources: Vec<DescribeConfigsResourceQuery>,
        include_synonyms: bool,
        include_documentation: bool,
    ) -> Self {
        Self {
            resources,
            include_synonyms,
            include_documentation,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.resources = self
            .resources
            .into_iter()
            .map(DescribeConfigsResourceQuery::canonicalize)
            .collect::<Vec<_>>()
            .into_boxed_slice()
            .into_vec();
        self
    }

    pub(crate) fn retention(&self) -> Option<DescribeConfigsRetention> {
        let resource_count = self.resources.len();
        let key_count = self.resources.iter().try_fold(0usize, |count, resource| {
            count.checked_add(resource.configuration_keys.as_ref().map_or(0, Vec::len))
        })?;
        let text_bytes = self.resources.iter().try_fold(0usize, |bytes, resource| {
            bytes.checked_add(resource.text_bytes()?)
        })?;
        let request =
            crate::admin::retention::request_charge(resource_count, key_count, text_bytes)?;
        let result_limit = resource_count.checked_mul(RESULT_BYTES_PER_RESOURCE)?;
        Some(DescribeConfigsRetention {
            total: request.checked_add(result_limit)?,
            result_limit,
        })
    }

    pub(crate) fn into_topic_plan(
        self,
    ) -> Result<DescribeConfigsPlan, DescribeConfigsRequestError> {
        let plan = DescribeConfigsPlan::new(
            self.resources
                .into_iter()
                .map(DescribeConfigsResourceQuery::into_core)
                .collect(),
            self.include_synonyms,
            self.include_documentation,
        )
        .map_err(DescribeConfigsRequestError::Invalid)?;
        if !topic_plan_supported(&plan) {
            return Err(DescribeConfigsRequestError::UnsupportedResource);
        }
        Ok(plan)
    }
}

pub(super) fn topic_plan_supported(plan: &DescribeConfigsPlan) -> bool {
    plan.resources()
        .iter()
        .all(|resource| resource.resource_type() == TOPIC_RESOURCE_TYPE)
}

pub(crate) enum DescribeConfigsRequestError {
    Invalid(DescribeConfigsPlanError),
    UnsupportedResource,
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}
