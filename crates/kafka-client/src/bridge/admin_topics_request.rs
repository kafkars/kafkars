//! Prepared engine request retained by inert topic-description builders.

use kafka_client_engine::DescribeTopicsRequest as EngineDescribeTopicsRequest;

/// Linear topic-description request translated only at the engine boundary.
pub(crate) struct DescribeTopicsAdminRequest {
    inner: EngineDescribeTopicsRequest,
}

impl DescribeTopicsAdminRequest {
    pub(crate) fn from_topics<I, T>(topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            inner: EngineDescribeTopicsRequest::new(topics.into_iter().map(Into::into).collect()),
        }
    }

    pub(crate) const fn all(include_internal: bool) -> Self {
        Self {
            inner: EngineDescribeTopicsRequest::all(include_internal),
        }
    }

    pub(crate) fn with_include_internal(mut self, include_internal: bool) -> Self {
        self.inner = EngineDescribeTopicsRequest::all(include_internal);
        self
    }

    pub(super) fn into_engine(self) -> EngineDescribeTopicsRequest {
        self.inner
    }
}

impl std::fmt::Debug for DescribeTopicsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeTopicsAdminRequest")
            .finish_non_exhaustive()
    }
}
