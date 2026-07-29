//! Prebuilt generic `DescribeConfigs` request retained by inert facade builders.

use kafka_client_engine::{
    DescribeConfigsRequest as EngineRequest, DescribeConfigsResourceQuery as EngineResourceQuery,
};

use crate::admin::{ConfigResourceQuery, TopicConfigQuery};

const TOPIC_RESOURCE_TYPE: i8 = 2;

/// Engine request prepared before the public submission boundary.
pub(crate) struct DescribeConfigsAdminRequest {
    inner: EngineRequest,
}

impl DescribeConfigsAdminRequest {
    pub(crate) fn from_topics<I, T>(topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<TopicConfigQuery>,
    {
        let resources = topics
            .into_iter()
            .map(Into::into)
            .map(|query| {
                let (topic, keys) = query.into_parts();
                EngineResourceQuery::new(TOPIC_RESOURCE_TYPE, topic, keys)
            })
            .collect();
        Self {
            inner: EngineRequest::new(resources, false, false),
        }
    }

    pub(crate) fn from_resources<I, T>(resources: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<ConfigResourceQuery>,
    {
        let resources = resources
            .into_iter()
            .map(Into::into)
            .map(|query| {
                let (resource_type, resource_name, keys) = query.into_parts();
                EngineResourceQuery::new(resource_type.as_raw(), resource_name, keys)
            })
            .collect();
        Self {
            inner: EngineRequest::new(resources, false, false),
        }
    }

    pub(crate) fn with_include_synonyms(mut self, include: bool) -> Self {
        self.inner = self.inner.with_include_synonyms(include);
        self
    }

    pub(crate) fn with_include_documentation(mut self, include: bool) -> Self {
        self.inner = self.inner.with_include_documentation(include);
        self
    }

    pub(crate) fn into_engine(self) -> EngineRequest {
        self.inner
    }
}

impl std::fmt::Debug for DescribeConfigsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeConfigsAdminRequest")
            .finish_non_exhaustive()
    }
}
