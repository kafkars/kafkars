//! Generated `DescribeConfigs` request construction from canonical borrowed queries.

use kafka_wire::{DescribeConfigsRequest, describe_configs_request::DescribeConfigsResource};

/// One already-validated resource query borrowed from its future policy owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DescribeConfigsQuery<'a> {
    pub(crate) resource_type: i8,
    pub(crate) resource_name: &'a str,
    pub(crate) configuration_keys: Option<&'a [&'a str]>,
}

/// Builds one generated request without acquiring routing or retry authority.
pub(crate) fn describe_configs_request(
    queries: &[DescribeConfigsQuery<'_>],
    include_synonyms: bool,
    include_documentation: bool,
) -> DescribeConfigsRequest {
    let resources = queries
        .iter()
        .map(|query| {
            let mut resource = DescribeConfigsResource::default();
            resource.resource_type = query.resource_type;
            resource.resource_name = query.resource_name.into();
            resource.configuration_keys = query
                .configuration_keys
                .map(|keys| keys.iter().map(|key| (*key).into()).collect());
            resource
        })
        .collect();
    let mut request = DescribeConfigsRequest::default();
    request.resources = resources;
    request.include_synonyms = include_synonyms;
    request.include_documentation = include_documentation;
    request
}
